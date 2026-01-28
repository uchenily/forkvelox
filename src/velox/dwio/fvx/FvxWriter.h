#pragma once

#include <cstddef>
#include <string>

#include "velox/vector/ComplexVector.h"

namespace facebook::velox::dwio::fvx {

struct FvxWriteOptions {
  size_t rowGroupSize = 1024;
};

class FvxWriter {
public:
  static void write(
      const RowVector& data,
      const std::string& path,
      FvxWriteOptions options = {});
};

} // namespace facebook::velox::dwio::fvx
