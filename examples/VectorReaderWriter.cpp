#include <cstring>
#include <iostream>
#include <vector>

#include "velox/buffer/Buffer.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/memory/Memory.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace {
int64_t serial() {
  static int64_t n = 0;
  return n++;
}
} // namespace

int main() {
  const int numRows = 8;
  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  auto intValues =
      AlignedBuffer::allocate(numRows * sizeof(int64_t), pool.get());
  auto *rawInts = intValues->asMutable<int64_t>();
  for (int i = 0; i < numRows; ++i) {
    rawInts[i] = serial();
  }
  auto intVector = std::make_shared<FlatVector<int64_t>>(
      pool.get(), BIGINT(), nullptr, numRows, intValues);

  std::vector<std::string> labels{"zero", "one",  "two", "three",
                                  "four", "five", "six", "seven"};
  auto stringValues =
      AlignedBuffer::allocate(numRows * sizeof(StringView), pool.get());
  auto *rawStrings = stringValues->asMutable<StringView>();
  size_t totalLen = 0;
  for (const auto &label : labels) {
    totalLen += label.size();
  }
  auto dataBuffer = AlignedBuffer::allocate(totalLen, pool.get());
  char *data = reinterpret_cast<char *>(dataBuffer->as_mutable_uint8_t());
  size_t offset = 0;
  for (int i = 0; i < numRows; ++i) {
    std::memcpy(data + offset, labels[i].data(), labels[i].size());
    rawStrings[i] = StringView(data + offset, labels[i].size());
    offset += labels[i].size();
  }

  auto stringNulls = AlignedBuffer::allocate(
      bits::nwords(numRows) * sizeof(uint64_t), pool.get());
  std::memset(stringNulls->as_mutable_uint8_t(), 0, stringNulls->capacity());
  bits::setBit(stringNulls->asMutable<uint64_t>(), 5, true);

  auto stringVector = std::make_shared<FlatVector<StringView>>(
      pool.get(), VARCHAR(), stringNulls, numRows, stringValues,
      std::vector<BufferPtr>{dataBuffer});

  auto rowVector = std::make_shared<RowVector>(
      pool.get(), ROW({"id", "label"}, {BIGINT(), VARCHAR()}), nullptr, numRows,
      std::vector<VectorPtr>{intVector, stringVector});

  std::cout << "RowVector content:" << std::endl;
  for (vector_size_t row = 0; row < rowVector->size(); ++row) {
    std::cout << rowVector->toString(row) << std::endl;
  }

  auto readInts =
      std::static_pointer_cast<FlatVector<int64_t>>(rowVector->childAt(0));
  auto readStrings =
      std::static_pointer_cast<FlatVector<StringView>>(rowVector->childAt(1));

  std::cout << "Reading rows:" << std::endl;
  for (vector_size_t row = 0; row < rowVector->size(); ++row) {
    std::cout << "row " << row << ": ";
    std::cout << "id=" << readInts->valueAt(row) << ", label=";
    if (readStrings->isNullAt(row)) {
      std::cout << "null";
    } else {
      std::cout << readStrings->valueAt(row);
    }
    std::cout << std::endl;
  }

  return 0;
}
