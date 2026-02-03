#include <cstring>
#include <folly/init/Init.h>
#include <iostream>

#include "velox/buffer/Buffer.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/fvx/FvxWriter.h"
#include "velox/dwio/fvx/RegisterFvxReader.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace {

RowVectorPtr makeSampleData(memory::MemoryPool *pool) {
  const vector_size_t numRows = 10;
  auto ids = AlignedBuffer::allocate(numRows * sizeof(int64_t), pool);
  auto *rawIds = ids->asMutable<int64_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawIds[i] = i + 1;
  }
  auto idVector = std::make_shared<FlatVector<int64_t>>(pool, BIGINT(), nullptr, numRows, ids);

  std::vector<std::string> labels{"alpha", "beta", "gamma", "delta", "skip", "omega", "pi", "tau", "sigma", "zeta"};
  auto labelValues = AlignedBuffer::allocate(numRows * sizeof(StringView), pool);
  auto *rawLabels = labelValues->asMutable<StringView>();
  size_t totalBytes = 0;
  for (const auto &label : labels) {
    totalBytes += label.size();
  }
  auto dataBuffer = AlignedBuffer::allocate(totalBytes, pool);
  char *data = dataBuffer->asMutable<char>();
  size_t offset = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    std::memcpy(data + offset, labels[i].data(), labels[i].size());
    rawLabels[i] = StringView(data + offset, labels[i].size());
    offset += labels[i].size();
  }
  auto labelVector = std::make_shared<FlatVector<StringView>>(pool, VARCHAR(), nullptr, numRows, labelValues,
                                                              std::vector<BufferPtr>{dataBuffer});

  auto prices = AlignedBuffer::allocate(numRows * sizeof(int32_t), pool);
  auto *rawPrices = prices->asMutable<int32_t>();
  for (vector_size_t i = 0; i < numRows; ++i) {
    rawPrices[i] = static_cast<int32_t>((i + 1) * 3);
  }
  auto priceVector = std::make_shared<FlatVector<int32_t>>(pool, INTEGER(), nullptr, numRows, prices);

  return std::make_shared<RowVector>(pool, ROW({"id", "label", "price"}, {BIGINT(), VARCHAR(), INTEGER()}), nullptr,
                                     numRows, std::vector<VectorPtr>{idVector, labelVector, priceVector});
}

} // namespace

int main(int argc, char **argv) {
  folly::init::Init init{&argc, &argv, false};
  filesystems::registerLocalFileSystem();
  dwio::fvx::registerFvxReaderFactory();
  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  const std::string filePath = (argc >= 2) ? std::string{argv[1]} : std::string{"data/sample.fvx"};

  auto data = makeSampleData(pool.get());
  dwio::fvx::FvxWriter::write(*data, filePath, {.rowGroupSize = 4});

  dwio::common::ReaderOptions readerOpts{pool.get()};
  readerOpts.setFileFormat(dwio::common::FileFormat::FVX);

  auto reader = dwio::common::getReaderFactory(dwio::common::FileFormat::FVX)
                    ->createReader(std::make_unique<dwio::common::BufferedInput>(
                                       std::make_shared<LocalReadFile>(filePath), readerOpts.memoryPool()),
                                   readerOpts);

  dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.setProjectedColumns({"id", "price"});
  rowReaderOptions.addFilter({"price", dwio::common::CompareOp::GT, Variant(12)});
  rowReaderOptions.addFilter({"label", dwio::common::CompareOp::NE, Variant("skip")});

  VectorPtr batch;
  auto rowReader = reader->createRowReader(rowReaderOptions);
  while (rowReader->next(3, batch)) {
    auto rowVector = std::dynamic_pointer_cast<RowVector>(batch);
    for (vector_size_t i = 0; i < rowVector->size(); ++i) {
      std::cout << rowVector->toString(i) << std::endl;
    }
  }

  return 0;
}
