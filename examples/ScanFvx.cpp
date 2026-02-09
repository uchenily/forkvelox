#include <array>
#include <cstring>
#include <folly/init/Init.h>
#include <iostream>
#include <string>
#include <vector>

#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
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

constexpr vector_size_t kRowGroupSize = 4;

std::string formatProjectedRow(int64_t id, int32_t price, const std::string &label) {
  return "{" + std::to_string(id) + ", " + std::to_string(price) + ", " + label + "}";
}

const char *compareOpToString(dwio::common::CompareOp op) {
  switch (op) {
  case dwio::common::CompareOp::EQ:
    return "=";
  case dwio::common::CompareOp::NE:
    return "!=";
  case dwio::common::CompareOp::LT:
    return "<";
  case dwio::common::CompareOp::LE:
    return "<=";
  case dwio::common::CompareOp::GT:
    return ">";
  case dwio::common::CompareOp::GE:
    return ">=";
  }
  return "?";
}

std::string formatFilterValue(const Variant &value) {
  if (value.isNull()) {
    return "null";
  }
  if (value.kind() == TypeKind::VARCHAR) {
    return "'" + value.value<std::string>() + "'";
  }
  return value.toString();
}

RowVectorPtr makeSampleData(memory::MemoryPool *pool) {
  constexpr std::array<int64_t, 10> kIds = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  constexpr std::array<int32_t, 10> kPrices = {3, 6, 9, 12, 15, 18, 21, 24, 27, 30};
  constexpr std::array<const char *, 10> kLabels = {"alpha", "beta", "gamma", "delta", "skip",
                                                     "omega", "pi",   "tau",   "sigma", "zeta"};
  const vector_size_t numRows = static_cast<vector_size_t>(kIds.size());
  VELOX_CHECK_EQ(kPrices.size(), kIds.size(), "Sample price array size mismatch");
  VELOX_CHECK_EQ(kLabels.size(), kIds.size(), "Sample label array size mismatch");

  auto ids = AlignedBuffer::allocate(numRows * sizeof(int64_t), pool);
  auto *rawIds = ids->asMutable<int64_t>();
  std::memcpy(rawIds, kIds.data(), numRows * sizeof(int64_t));
  auto idVector = std::make_shared<FlatVector<int64_t>>(pool, BIGINT(), nullptr, numRows, ids);

  auto labelValues = AlignedBuffer::allocate(numRows * sizeof(StringView), pool);
  auto *rawLabels = labelValues->asMutable<StringView>();
  size_t totalBytes = 0;
  for (const auto *label : kLabels) {
    totalBytes += std::strlen(label);
  }
  auto dataBuffer = AlignedBuffer::allocate(totalBytes, pool);
  char *data = dataBuffer->asMutable<char>();
  size_t offset = 0;
  for (vector_size_t i = 0; i < numRows; ++i) {
    const auto *label = kLabels[static_cast<size_t>(i)];
    const auto labelSize = std::strlen(label);
    std::memcpy(data + offset, label, labelSize);
    rawLabels[i] = StringView(data + offset, labelSize);
    offset += labelSize;
  }
  auto labelVector = std::make_shared<FlatVector<StringView>>(pool, VARCHAR(), nullptr, numRows, labelValues,
                                                              std::vector<BufferPtr>{dataBuffer});

  auto prices = AlignedBuffer::allocate(numRows * sizeof(int32_t), pool);
  auto *rawPrices = prices->asMutable<int32_t>();
  std::memcpy(rawPrices, kPrices.data(), numRows * sizeof(int32_t));
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
  std::cout << data->toString() << std::endl;
  dwio::fvx::FvxWriter::write(*data, filePath, {.rowGroupSize = kRowGroupSize});

  dwio::common::ReaderOptions readerOpts{pool.get()};
  readerOpts.setFileFormat(dwio::common::FileFormat::FVX);

  auto reader = dwio::common::getReaderFactory(dwio::common::FileFormat::FVX)
                    ->createReader(std::make_unique<dwio::common::BufferedInput>(
                                       std::make_shared<LocalReadFile>(filePath), readerOpts.memoryPool()),
                                   readerOpts);

  dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.setProjectedColumns({"id", "price", "label"});
  rowReaderOptions.addFilter({"price", dwio::common::CompareOp::GT, Variant(12)});
  rowReaderOptions.addFilter({"label", dwio::common::CompareOp::NE, Variant("skip")});
  std::cout << "Filters:" << std::endl;
  for (const auto &filter : rowReaderOptions.filters()) {
    std::cout << "  " << filter.column << " " << compareOpToString(filter.op) << " " << formatFilterValue(filter.value)
              << std::endl;
  }
  std::cout << std::endl;

  auto *inputIds = data->childAt(0)->asChecked<FlatVector<int64_t>>();
  auto *inputLabels = data->childAt(1)->asChecked<FlatVector<StringView>>();
  auto *inputPrices = data->childAt(2)->asChecked<FlatVector<int32_t>>();
  std::vector<std::string> expectedRows;
  expectedRows.reserve(data->size());
  for (vector_size_t i = 0; i < data->size(); ++i) {
    auto price = inputPrices->valueAt(i);
    std::string label{inputLabels->valueAt(i).str()};
    if (price > 12 && label != "skip") {
      expectedRows.push_back(formatProjectedRow(inputIds->valueAt(i), price, label));
    }
  }

  VectorPtr batch;
  auto rowReader = reader->createRowReader(rowReaderOptions);
  std::vector<std::string> actualRows;
  while (rowReader->next(3, batch)) {
    auto *rowVector = batch->asChecked<RowVector>();
    for (vector_size_t i = 0; i < rowVector->size(); ++i) {
      auto row = rowVector->toString(i);
      std::cout << row << std::endl;
      actualRows.push_back(row);
    }
  }
  std::cout.flush();

  if (actualRows != expectedRows) {
    std::cout << "Detection failed: scan result does not match row-level filter expectation." << std::endl;
    std::cout << "Expected row count: " << expectedRows.size() << ", actual row count: " << actualRows.size()
              << std::endl;
    std::cout << "Expected rows:" << std::endl;
    for (const auto &row : expectedRows) {
      std::cout << row << std::endl;
    }
    std::cout << "Actual rows:" << std::endl;
    for (const auto &row : actualRows) {
      std::cout << row << std::endl;
    }
    return 1;
  }
  std::cout << "Detection passed: " << actualRows.size() << " rows matched expected results." << std::endl;

  return 0;
}
