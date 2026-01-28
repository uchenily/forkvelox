#include "velox/dwio/fvx/RegisterFvxReader.h"

#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/fvx/FvxReader.h"

namespace facebook::velox::dwio::fvx {
namespace {

class FvxRowReaderAdapter : public dwio::common::RowReader {
public:
  explicit FvxRowReaderAdapter(std::unique_ptr<FvxRowReader> reader)
      : reader_(std::move(reader)) {}

  bool next(size_t batchSize, VectorPtr& out) override {
    RowVectorPtr batch;
    if (!reader_->next(batchSize, batch)) {
      return false;
    }
    out = batch;
    return true;
  }

private:
  std::unique_ptr<FvxRowReader> reader_;
};

class FvxReaderAdapter : public dwio::common::Reader {
public:
  explicit FvxReaderAdapter(std::unique_ptr<FvxReader> reader)
      : reader_(std::move(reader)) {}

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options) override {
    return std::make_unique<FvxRowReaderAdapter>(
        reader_->createRowReader(options));
  }

private:
  std::unique_ptr<FvxReader> reader_;
};

class FvxReaderFactory : public dwio::common::ReaderFactory {
public:
  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options) override {
    auto reader = std::make_unique<FvxReader>(
        input->file(), options.memoryPool());
    return std::make_unique<FvxReaderAdapter>(std::move(reader));
  }
};

} // namespace

void registerFvxReaderFactory() {
  dwio::common::registerReaderFactory(
      dwio::common::FileFormat::FVX,
      std::make_unique<FvxReaderFactory>());
}

} // namespace facebook::velox::dwio::fvx
