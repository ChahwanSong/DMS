#include "dms/checksum.hpp"

#include <cassert>
#include <vector>

int main() {
    std::vector<char> data{'a', 'b', 'c'};
    auto crc = dms::Checksum::crc32(data);
    auto hex = dms::Checksum::crc32_hex(data);
    assert(crc == 0x352441C2u);
    assert(hex == "352441c2");
    dms::Checksum::Crc32Accumulator accumulator;
    accumulator.update(data.data(), 2);
    accumulator.update(data.data() + 2, data.size() - 2);
    assert(accumulator.value() == crc);
    assert(accumulator.hex() == hex);
    return 0;
}
