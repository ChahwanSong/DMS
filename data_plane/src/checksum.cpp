#include "dms/checksum.hpp"

#include <array>
#include <iomanip>
#include <sstream>

namespace dms {

namespace {

constexpr std::uint32_t polynomial = 0xEDB88320u;

constexpr std::array<std::uint32_t, 256> make_crc32_table() {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t i = 0; i < table.size(); ++i) {
        std::uint32_t value = i;
        for (std::uint32_t j = 0; j < 8; ++j) {
            if (value & 1u) {
                value = (value >> 1) ^ polynomial;
            } else {
                value >>= 1;
            }
        }
        table[i] = value;
    }
    return table;
}

const std::array<std::uint32_t, 256> &crc32_table() {
    static const auto table = make_crc32_table();
    return table;
}

} // namespace

std::uint32_t Checksum::crc32(const std::vector<char> &data) {
    const auto &table = crc32_table();
    std::uint32_t crc = 0xFFFFFFFFu;
    for (unsigned char byte : data) {
        crc = (crc >> 8) ^ table[(crc ^ byte) & 0xFFu];
    }
    return crc ^ 0xFFFFFFFFu;
}

std::string Checksum::crc32_hex(const std::vector<char> &data) {
    std::uint32_t value = crc32(data);
    std::ostringstream oss;
    oss << std::hex << std::nouppercase << std::setfill('0') << std::setw(8) << value;
    return oss.str();
}

Checksum::Crc32Accumulator::Crc32Accumulator() : crc_(0xFFFFFFFFu) {}

void Checksum::Crc32Accumulator::update(const char *data, std::size_t size) {
    if (data == nullptr || size == 0) {
        return;
    }
    const auto &table = crc32_table();
    for (std::size_t i = 0; i < size; ++i) {
        unsigned char byte = static_cast<unsigned char>(data[i]);
        crc_ = (crc_ >> 8) ^ table[(crc_ ^ byte) & 0xFFu];
    }
}

std::uint32_t Checksum::Crc32Accumulator::value() const {
    return crc_ ^ 0xFFFFFFFFu;
}

std::string Checksum::Crc32Accumulator::hex() const {
    std::ostringstream oss;
    oss << std::hex << std::nouppercase << std::setfill('0') << std::setw(8) << value();
    return oss.str();
}

} // namespace dms
