#pragma once

#include <memory>
#include <string>

#include "velox/common/memory/MemoryPool.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::dwio::common {

enum class FileFormat {
  ORC,
  DWRF,
  CSV,
};

class LocalReadFile {
public:
  explicit LocalReadFile(std::string path) : path_(std::move(path)) {}
  const std::string& path() const { return path_; }

private:
  std::string path_;
};

class BufferedInput {
public:
  BufferedInput(
      std::shared_ptr<LocalReadFile> file,
      memory::MemoryPool* pool)
      : file_(std::move(file)), pool_(pool) {}

  const std::string& path() const { return file_->path(); }
  memory::MemoryPool* memoryPool() const { return pool_; }

private:
  std::shared_ptr<LocalReadFile> file_;
  memory::MemoryPool* pool_;
};

class ReaderOptions {
public:
  explicit ReaderOptions(memory::MemoryPool* pool) : pool_(pool) {}

  void setFileFormat(FileFormat format) { format_ = format; }
  FileFormat fileFormat() const { return format_; }
  memory::MemoryPool* memoryPool() const { return pool_; }

private:
  memory::MemoryPool* pool_;
  FileFormat format_{FileFormat::ORC};
};

class RowReaderOptions {};

class RowReader {
public:
  virtual ~RowReader() = default;
  virtual bool next(size_t batchSize, VectorPtr& out) = 0;
};

class Reader {
public:
  virtual ~Reader() = default;
  virtual std::unique_ptr<RowReader> createRowReader(
      const RowReaderOptions& options) = 0;
};

class ReaderFactory {
public:
  virtual ~ReaderFactory() = default;
  virtual std::unique_ptr<Reader> createReader(
      std::unique_ptr<BufferedInput> input,
      const ReaderOptions& options) = 0;
};

} // namespace facebook::velox::dwio::common
