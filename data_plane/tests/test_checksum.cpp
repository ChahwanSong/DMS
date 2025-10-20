#include "dms/checksum.hpp"

#include <cassert>
#include <vector>

int main() {
    std::vector<char> data{'a', 'b', 'c'};
    auto crc = dms::Checksum::crc32(data);
    auto hex = dms::Checksum::crc32_hex(data);
    assert(crc == 0x352441C2u);
    assert(hex == "352441c2");
    return 0;
}
