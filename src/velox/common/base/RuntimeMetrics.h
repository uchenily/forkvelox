#pragma once
#include <cstdint>
#include <string>
#include <unordered_map>

namespace facebook::velox {
struct RuntimeCounter {
  std::string unit;
  uint64_t value;
};
class RuntimeMetric {
public:
  explicit RuntimeMetric(std::string unit) : unit(unit) {}
  void addValue(uint64_t val) {}
  void merge(const RuntimeMetric &other) {}
  std::string unit;
};
} // namespace facebook::velox
