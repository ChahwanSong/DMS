#include "dms/transfer_manager.hpp"

#include "dms/checksum.hpp"

#include <fstream>
#include <iostream>

namespace dms {

TransferManager::TransferManager(std::size_t chunk_size_bytes, std::size_t concurrency,
                                 NetworkTransport &transport)
    : chunker_(chunk_size_bytes), concurrency_(concurrency), transport_(transport) {
    if (concurrency_ == 0) {
        throw std::invalid_argument("concurrency must be > 0");
    }
    threads_.reserve(concurrency_);
    for (std::size_t i = 0; i < concurrency_; ++i) {
        threads_.emplace_back(&TransferManager::worker_thread, this);
    }
}

void TransferManager::submit_job(const TransferJob &job) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        jobs_.push(job);
    }
    cv_.notify_one();
}

void TransferManager::wait_for_completion() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
    for (auto &thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TransferManager::worker_thread() {
    while (true) {
        TransferJob job;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [&] { return stop_ || !jobs_.empty(); });
            if (stop_ && jobs_.empty()) {
                break;
            }
            job = jobs_.front();
            jobs_.pop();
        }
        auto files = FileChunker::enumerate_files(job.source);
        for (const auto &file : files) {
            auto checksum = compute_file_checksum(file);
            if (!checksum) {
                continue;
            }
            auto chunks = chunker_.chunk_file(file);
            for (const auto &chunk : chunks) {
                process_chunk(chunk, job.destination, *checksum);
            }
        }
    }
}

void TransferManager::process_chunk(const FileChunk &chunk, const NetworkEndpoint &endpoint,
                                    const std::string &file_checksum) {
    std::ifstream file(chunk.path, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file: " << chunk.path << std::endl;
        return;
    }
    file.seekg(static_cast<std::streamoff>(chunk.offset));
    std::vector<char> buffer(chunk.size);
    file.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    buffer.resize(static_cast<std::size_t>(file.gcount()));
    ChunkPayload payload{chunk.path, chunk.offset, std::move(buffer), file_checksum};
    transport_.send_chunk(endpoint, std::move(payload));
}

std::optional<std::string> TransferManager::compute_file_checksum(const std::filesystem::path &path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file for checksum: " << path << std::endl;
        return std::nullopt;
    }
    Checksum::Crc32Accumulator accumulator;
    std::size_t buffer_size = chunker_.chunk_size_bytes();
    if (buffer_size == 0) {
        buffer_size = 4096;
    } else if (buffer_size > (1u << 20)) {
        buffer_size = 1u << 20;
    }
    std::vector<char> buffer(buffer_size);
    while (file) {
        file.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        auto read = file.gcount();
        if (read <= 0) {
            break;
        }
        accumulator.update(buffer.data(), static_cast<std::size_t>(read));
    }
    return accumulator.hex();
}

} // namespace dms
