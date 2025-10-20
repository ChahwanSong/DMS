#include "dms/file_chunker.hpp"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <vector>

int main() {
    namespace fs = std::filesystem;
    auto temp_dir = fs::temp_directory_path() / "dms_test";
    fs::create_directories(temp_dir);
    auto file_path = temp_dir / "file.bin";
    {
        std::ofstream file(file_path, std::ios::binary);
        std::vector<char> data(1024, '\x01');
        file.write(data.data(), static_cast<std::streamsize>(data.size()));
    }
    dms::FileChunker chunker(256);
    auto chunks = chunker.chunk_file(file_path);
    assert(chunks.size() == 4);
    auto files = dms::FileChunker::enumerate_files(temp_dir);
    assert(files.size() == 1);
    fs::remove_all(temp_dir);
    return 0;
}
