#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace dms {

struct FileChunk {
    std::filesystem::path path;
    std::uint64_t offset;
    std::size_t size;
};

class FileChunker {
  public:
    explicit FileChunker(std::size_t chunk_size_bytes);

    std::vector<FileChunk> chunk_file(const std::filesystem::path &path) const;

    static std::vector<std::filesystem::path> enumerate_files(const std::filesystem::path &root);

    std::size_t chunk_size_bytes() const noexcept;

  private:
    std::size_t chunk_size_bytes_;
};

} // namespace dms
