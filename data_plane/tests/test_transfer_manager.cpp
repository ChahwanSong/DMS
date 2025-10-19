#include "dms/transfer_manager.hpp"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <mutex>

namespace {

class RecordingTransport : public dms::NetworkTransport {
  public:
    void send_chunk(const dms::NetworkEndpoint &, const std::vector<char> &buffer) override {
        std::lock_guard<std::mutex> lock(mutex_);
        total_bytes_ += buffer.size();
        ++chunks_;
    }

    std::size_t total_bytes() const { return total_bytes_; }
    std::size_t chunks() const { return chunks_; }

  private:
    std::size_t total_bytes_{0};
    std::size_t chunks_{0};
    std::mutex mutex_;
};

} // namespace

int main() {
    namespace fs = std::filesystem;
    auto temp_dir = fs::temp_directory_path() / "dms_tm_test";
    fs::create_directories(temp_dir);
    auto file_path = temp_dir / "data.bin";
    {
        std::ofstream file(file_path, std::ios::binary);
        std::vector<char> data(4096, '\x02');
        file.write(data.data(), static_cast<std::streamsize>(data.size()));
    }
    RecordingTransport transport;
    dms::TransferManager manager(512, 2, transport);
    dms::TransferJob job{file_path, {"127.0.0.1", 9000, "eth0"}};
    manager.submit_job(job);
    manager.wait_for_completion();
    assert(transport.chunks() > 0);
    assert(transport.total_bytes() == 4096);
    fs::remove_all(temp_dir);
    return 0;
}
