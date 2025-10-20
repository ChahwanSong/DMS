#include "dms/network.hpp"

#include <iostream>

namespace dms {

void TcpTransport::send_chunk(const NetworkEndpoint &endpoint, ChunkPayload payload) {
    std::cout << "Sending chunk to " << endpoint.address << ':' << endpoint.port
              << " via interface " << endpoint.interface_name
              << " file_checksum=" << payload.file_checksum_hex
              << " bytes=" << payload.data.size() << std::endl;
}

} // namespace dms
