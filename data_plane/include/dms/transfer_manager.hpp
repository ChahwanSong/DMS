#pragma once

#include "dms/file_chunker.hpp"
#include "dms/network.hpp"

#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace dms {

struct TransferJob {
    std::filesystem::path source;
    NetworkEndpoint destination;
};

class TransferManager {
  public:
    TransferManager(std::size_t chunk_size_bytes, std::size_t concurrency, NetworkTransport &transport);

    void submit_job(const TransferJob &job);

    void wait_for_completion();

  private:
    void worker_thread();
    void process_chunk(const FileChunk &chunk, const NetworkEndpoint &endpoint,
                       const std::string &file_checksum);
    std::optional<std::string> compute_file_checksum(const std::filesystem::path &path);

    FileChunker chunker_;
    std::size_t concurrency_;
    NetworkTransport &transport_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<TransferJob> jobs_;
    bool stop_{false};
    std::vector<std::thread> threads_;
};

} // namespace dms
