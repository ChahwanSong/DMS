#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dms {

class Checksum {
  public:
    static std::uint32_t crc32(const std::vector<char> &data);

    static std::string crc32_hex(const std::vector<char> &data);
};

} // namespace dms
