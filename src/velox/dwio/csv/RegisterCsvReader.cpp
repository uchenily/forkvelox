#include "velox/dwio/csv/RegisterCsvReader.h"

#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/csv/CsvReader.h"

namespace facebook::velox::dwio::csv {
namespace {

class CsvRowReaderAdapter : public dwio::common::RowReader {
public:
  explicit CsvRowReaderAdapter(std::unique_ptr<CsvRowReader> reader) : reader_(std::move(reader)) {}

  bool next(size_t batchSize, VectorPtr &out) override {
    RowVectorPtr batch;
    if (!reader_->next(batchSize, batch)) {
      return false;
    }
    out = batch;
    return true;
  }

private:
  std::unique_ptr<CsvRowReader> reader_;
};

class CsvReaderAdapter : public dwio::common::Reader {
public:
  explicit CsvReaderAdapter(std::unique_ptr<CsvReader> reader) : reader_(std::move(reader)) {}

  std::unique_ptr<dwio::common::RowReader> createRowReader(const dwio::common::RowReaderOptions &options) override {
    return std::make_unique<CsvRowReaderAdapter>(reader_->createRowReader());
  }

private:
  std::unique_ptr<CsvReader> reader_;
};

class CsvReaderFactory : public dwio::common::ReaderFactory {
public:
  std::unique_ptr<dwio::common::Reader> createReader(std::unique_ptr<dwio::common::BufferedInput> input,
                                                     const dwio::common::ReaderOptions &options) override {
    CsvReadOptions csvOptions;
    auto reader = std::make_unique<CsvReader>(input->file(), options.memoryPool(), csvOptions);
    return std::make_unique<CsvReaderAdapter>(std::move(reader));
  }
};

} // namespace

void registerCsvReaderFactory() {
  dwio::common::registerReaderFactory(dwio::common::FileFormat::CSV, std::make_unique<CsvReaderFactory>());
}

} // namespace facebook::velox::dwio::csv
