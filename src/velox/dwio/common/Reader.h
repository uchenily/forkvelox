#pragma once

#include <memory>
#include <string>
#include <vector>

#include "velox/common/file/File.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/type/Variant.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::dwio::common {

enum class FileFormat {
  ORC,
  DWRF,
  CSV,
  FVX,
};

class BufferedInput {
public:
  BufferedInput(std::shared_ptr<ReadFile> file, memory::MemoryPool *pool)
      : file_(std::move(file)), pool_(pool) {}

  std::string path() const { return file_->getName(); }
  const std::shared_ptr<ReadFile> &file() const { return file_; }
  memory::MemoryPool *memoryPool() const { return pool_; }

private:
  std::shared_ptr<ReadFile> file_;
  memory::MemoryPool *pool_;
};

class ReaderOptions {
public:
  explicit ReaderOptions(memory::MemoryPool *pool) : pool_(pool) {}

  void setFileFormat(FileFormat format) { format_ = format; }
  FileFormat fileFormat() const { return format_; }
  memory::MemoryPool *memoryPool() const { return pool_; }

private:
  memory::MemoryPool *pool_;
  FileFormat format_{FileFormat::ORC};
};

enum class CompareOp {
  EQ,
  NE,
  LT,
  LE,
  GT,
  GE,
};

struct FilterCondition {
  std::string column;
  CompareOp op;
  Variant value;
};

class RowReaderOptions {
public:
  void addFilter(FilterCondition filter) {
    filters_.push_back(std::move(filter));
  }

  void setProjectedColumns(std::vector<std::string> columns) {
    projectedColumns_ = std::move(columns);
  }

  const std::vector<FilterCondition> &filters() const { return filters_; }
  const std::vector<std::string> &projectedColumns() const {
    return projectedColumns_;
  }
  bool hasProjection() const { return !projectedColumns_.empty(); }

private:
  std::vector<FilterCondition> filters_;
  std::vector<std::string> projectedColumns_;
};

class RowReader {
public:
  virtual ~RowReader() = default;
  virtual bool next(size_t batchSize, VectorPtr &out) = 0;
};

class Reader {
public:
  virtual ~Reader() = default;
  virtual std::unique_ptr<RowReader>
  createRowReader(const RowReaderOptions &options) = 0;
};

class ReaderFactory {
public:
  virtual ~ReaderFactory() = default;
  virtual std::unique_ptr<Reader>
  createReader(std::unique_ptr<BufferedInput> input,
               const ReaderOptions &options) = 0;
};

} // namespace facebook::velox::dwio::common
