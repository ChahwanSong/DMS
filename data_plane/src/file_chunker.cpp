#include "dms/file_chunker.hpp"

#include <fstream>
#include <stdexcept>

namespace dms {

FileChunker::FileChunker(std::size_t chunk_size_bytes) : chunk_size_bytes_(chunk_size_bytes) {
    if (chunk_size_bytes_ == 0) {
        throw std::invalid_argument("chunk size must be > 0");
    }
}

std::vector<FileChunk> FileChunker::chunk_file(const std::filesystem::path &path) const {
    if (!std::filesystem::is_regular_file(path)) {
        throw std::invalid_argument("path must be a regular file: " + path.string());
    }
    const auto file_size = std::filesystem::file_size(path);
    std::vector<FileChunk> chunks;
    chunks.reserve(static_cast<std::size_t>(file_size / chunk_size_bytes_) + 1);
    for (std::uint64_t offset = 0; offset < file_size; offset += chunk_size_bytes_) {
        const auto remaining = file_size - offset;
        const auto size = static_cast<std::size_t>(std::min<std::uint64_t>(remaining, chunk_size_bytes_));
        chunks.push_back(FileChunk{path, offset, size});
    }
    if (chunks.empty()) {
        chunks.push_back(FileChunk{path, 0, 0});
    }
    return chunks;
}

std::vector<std::filesystem::path> FileChunker::enumerate_files(const std::filesystem::path &root) {
    std::vector<std::filesystem::path> files;
    if (!std::filesystem::exists(root)) {
        throw std::invalid_argument("root does not exist: " + root.string());
    }
    if (std::filesystem::is_regular_file(root)) {
        files.push_back(root);
        return files;
    }
    for (auto const &entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file()) {
            files.push_back(entry.path());
        }
    }
    return files;
}

std::size_t FileChunker::chunk_size_bytes() const noexcept { return chunk_size_bytes_; }

} // namespace dms
