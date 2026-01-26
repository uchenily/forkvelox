#pragma once
#include <cstdint>
#include <string>
#include <string_view>

namespace facebook::velox::common {

struct Region {
  uint64_t offset;
  uint64_t length;
  std::string_view label;

  Region(uint64_t offset = 0, uint64_t length = 0, std::string_view label = {})
      : offset{offset}, length{length}, label{label} {}
};

} // namespace facebook::velox::common
