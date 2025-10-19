#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace dms {

struct NetworkEndpoint {
    std::string address;
    std::uint16_t port;
    std::string interface_name;
};

class NetworkTransport {
  public:
    virtual ~NetworkTransport() = default;

    virtual void send_chunk(const NetworkEndpoint &endpoint, const std::vector<char> &buffer) = 0;
};

class TcpTransport : public NetworkTransport {
  public:
    void send_chunk(const NetworkEndpoint &endpoint, const std::vector<char> &buffer) override;
};

} // namespace dms
