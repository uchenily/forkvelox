#pragma once
#include <memory>
#include <string>
#include <cstdint>

namespace facebook::velox {

class FileIoTracer {
 public:
  virtual ~FileIoTracer() = default;
};

} // namespace facebook::velox
