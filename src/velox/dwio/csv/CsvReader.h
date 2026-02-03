#pragma once

#include <memory>
#include <string>

#include "velox/common/file/File.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::dwio::csv {

struct CsvReadOptions {
  char delimiter = ',';
  bool hasHeader = true;
  bool hasTypes = true;
  bool inferTypesIfMissing = true;
};

class CsvRowReader;

class CsvReader {
public:
  CsvReader(std::shared_ptr<ReadFile> file, memory::MemoryPool *pool,
            CsvReadOptions options = {});

  std::unique_ptr<CsvRowReader> createRowReader() const;
  const RowTypePtr &rowType() const { return rowType_; }

private:
  std::shared_ptr<ReadFile> file_;
  memory::MemoryPool *pool_;
  CsvReadOptions options_;
  RowTypePtr rowType_;
  std::string firstDataLine_;
  bool hasFirstDataLine_ = false;
};

class CsvRowReader {
public:
  CsvRowReader(std::shared_ptr<ReadFile> file, RowTypePtr rowType,
               memory::MemoryPool *pool, CsvReadOptions options,
               std::string firstDataLine, bool hasFirstDataLine);

  bool next(size_t batchSize, RowVectorPtr &out);

private:
  std::shared_ptr<ReadFile> file_;
  RowTypePtr rowType_;
  memory::MemoryPool *pool_;
  CsvReadOptions options_;
  std::unique_ptr<std::istream> input_;
  std::string firstDataLine_;
  bool hasFirstDataLine_ = false;
  bool initialized_ = false;
  bool eof_ = false;

  void ensureInitialized();
};

} // namespace facebook::velox::dwio::csv
