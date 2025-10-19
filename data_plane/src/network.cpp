#include "dms/network.hpp"

#include <chrono>
#include <iostream>
#include <thread>

namespace dms {

void TcpTransport::send_chunk(const NetworkEndpoint &endpoint, const std::vector<char> &buffer) {
    // Placeholder implementation simulating transmission delay.
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(std::chrono::microseconds(buffer.size()));
    std::cout << "Sent chunk of size " << buffer.size() << " bytes to " << endpoint.address << ":"
              << endpoint.port << " via iface " << endpoint.interface_name << std::endl;
}

} // namespace dms
