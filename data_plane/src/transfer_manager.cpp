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
            auto chunks = chunker_.chunk_file(file);
            for (const auto &chunk : chunks) {
                process_chunk(chunk, job.destination);
            }
        }
    }
}

void TransferManager::process_chunk(const FileChunk &chunk, const NetworkEndpoint &endpoint) {
    std::ifstream file(chunk.path, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open file: " << chunk.path << std::endl;
        return;
    }
    file.seekg(static_cast<std::streamoff>(chunk.offset));
    std::vector<char> buffer(chunk.size);
    file.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    buffer.resize(static_cast<std::size_t>(file.gcount()));
    auto checksum = Checksum::crc32_hex(buffer);
    ChunkPayload payload{chunk.path, chunk.offset, std::move(buffer), std::move(checksum)};
    transport_.send_chunk(endpoint, std::move(payload));
}

} // namespace dms
