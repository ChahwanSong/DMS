#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace dms {

struct NetworkEndpoint {
    std::string address;
    std::uint16_t port;
    std::string interface_name;
};

struct ChunkPayload {
    std::filesystem::path path;
    std::uint64_t offset;
    std::vector<char> data;
    std::string checksum_hex;
};

class NetworkTransport {
  public:
    virtual ~NetworkTransport() = default;

    virtual void send_chunk(const NetworkEndpoint &endpoint, ChunkPayload payload) = 0;
};

class TcpTransport : public NetworkTransport {
  public:
    void send_chunk(const NetworkEndpoint &endpoint, ChunkPayload payload) override;
};

} // namespace dms
