#pragma once

#include <fstream>
#include <memory>
#include <string>

#include "velox/common/memory/MemoryPool.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::io {

struct CsvReadOptions {
  char delimiter = ',';
  bool hasHeader = true;
  bool hasTypes = true;
  bool inferTypesIfMissing = true;
};

class CsvRowReader;

class CsvReader {
public:
  CsvReader(
      std::string path,
      memory::MemoryPool* pool,
      CsvReadOptions options = {});

  std::unique_ptr<CsvRowReader> createRowReader() const;
  const RowTypePtr& rowType() const { return rowType_; }

private:
  std::string path_;
  memory::MemoryPool* pool_;
  CsvReadOptions options_;
  RowTypePtr rowType_;
};

class CsvRowReader {
public:
  CsvRowReader(
      std::string path,
      RowTypePtr rowType,
      memory::MemoryPool* pool,
      CsvReadOptions options);

  bool next(size_t batchSize, RowVectorPtr& out);

private:
  std::string path_;
  RowTypePtr rowType_;
  memory::MemoryPool* pool_;
  CsvReadOptions options_;
  std::unique_ptr<std::ifstream> file_;
  bool initialized_ = false;
  bool eof_ = false;

  void ensureInitialized();
};

} // namespace facebook::velox::io
