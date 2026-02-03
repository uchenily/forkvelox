#pragma once

#include "velox/common/memory/MemoryPool.h"
#include "velox/vector/ComplexVector.h"
#include <string>

namespace facebook::velox::dwio::common {

class RowVectorFile {
public:
  static void write(const RowVector &data, const std::string &path);
  static void append(const RowVector &data, const std::string &path,
                     bool includeHeader);
  static RowVectorPtr read(memory::MemoryPool *pool, const std::string &path);
};

} // namespace facebook::velox::dwio::common
