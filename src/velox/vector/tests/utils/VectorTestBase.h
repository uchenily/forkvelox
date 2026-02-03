#pragma once
#include "velox/common/memory/Memory.h"
#include "velox/core/ExecCtx.h"
#include "velox/core/QueryCtx.h"
#include "velox/type/StringView.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::test {

class VectorTestBase {
public:
  VectorTestBase() {
    memory::MemoryManager::initialize({});
    pool_ = memory::defaultMemoryPool();
    queryCtx_ = core::QueryCtx::create();
    execCtx_ = std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get());
  }

  virtual ~VectorTestBase() = default;

  memory::MemoryPool *pool() { return pool_.get(); }

  std::shared_ptr<FlatVector<StringView>>
  makeFlatVectorString(const std::vector<std::string> &data) {
    size_t size = data.size();
    auto values = AlignedBuffer::allocate(size * sizeof(StringView), pool());
    auto *rawValues = values->asMutable<StringView>();

    size_t totalLen = 0;
    for (const auto &s : data)
      totalLen += s.size();

    auto dataBuffer = AlignedBuffer::allocate(totalLen, pool());
    char *bufPtr = dataBuffer->asMutable<char>();
    size_t offset = 0;

    for (size_t i = 0; i < size; ++i) {
      std::memcpy(bufPtr + offset, data[i].data(), data[i].size());
      rawValues[i] = StringView(bufPtr + offset, data[i].size());
      offset += data[i].size();
    }

    auto vec = std::make_shared<FlatVector<StringView>>(pool(), VARCHAR(),
                                                        nullptr, size, values);
    vec->addStringBuffer(dataBuffer);
    return vec;
  }

  template <typename T> auto makeFlatVector(const std::vector<T> &data) {
    if constexpr (std::is_same_v<T, std::string>) {
      return makeFlatVectorString(data);
    } else {
      size_t size = data.size();
      size_t bytes = size * sizeof(T);
      auto buffer = AlignedBuffer::allocate(bytes, pool());
      if (size > 0) {
        std::memcpy(buffer->as_mutable_uint8_t(), data.data(), bytes);
      }

      std::shared_ptr<const Type> type;
      if constexpr (std::is_same_v<T, int64_t>)
        type = BIGINT();
      else if constexpr (std::is_same_v<T, int32_t>)
        type = INTEGER();
      else
        type = BIGINT();

      return std::make_shared<FlatVector<T>>(pool(), type, nullptr, size,
                                             buffer);
    }
  }

  std::shared_ptr<RowVector>
  makeRowVector(const std::vector<std::string> &names,
                const std::vector<VectorPtr> &children) {
    std::vector<TypePtr> types;
    for (auto &c : children)
      types.push_back(c->type());
    auto rowType = ROW(names, types);
    return std::make_shared<RowVector>(pool(), rowType, nullptr,
                                       children[0]->size(), children);
  }

  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
  std::unique_ptr<core::ExecCtx> execCtx_;
};

} // namespace facebook::velox::test
