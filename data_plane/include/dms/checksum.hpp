#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace dms {

class Checksum {
  public:
    static std::uint32_t crc32(const std::vector<char> &data);

    static std::string crc32_hex(const std::vector<char> &data);

    class Crc32Accumulator {
      public:
        Crc32Accumulator();

        void update(const char *data, std::size_t size);

        std::uint32_t value() const;

        std::string hex() const;

      private:
        std::uint32_t crc_;
    };
};

} // namespace dms
