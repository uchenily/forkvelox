#pragma once

#include <string>
#include "velox/vector/ComplexVector.h"
#include "velox/common/memory/MemoryPool.h"

namespace facebook::velox::io {

class RowVectorFile {
public:
  static void write(const RowVector& data, const std::string& path);
  static void append(const RowVector& data, const std::string& path, bool includeHeader);
  static RowVectorPtr read(memory::MemoryPool* pool, const std::string& path);
};

} // namespace facebook::velox::io
