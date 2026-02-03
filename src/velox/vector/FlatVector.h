#pragma once

#include "velox/buffer/Buffer.h"
#include "velox/type/StringView.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox {

template <typename T>
class FlatVector : public SimpleVector<T> {
public:
  FlatVector(memory::MemoryPool *pool, std::shared_ptr<const Type> type, BufferPtr nulls, vector_size_t length,
             BufferPtr values, std::vector<BufferPtr> stringBuffers = {})
      : SimpleVector<T>(pool, type, VectorEncoding::Simple::FLAT, std::move(nulls), length), values_(std::move(values)),
        stringBuffers_(std::move(stringBuffers)) {

    if (values_) {
      rawValues_ = values_->as<T>();
    } else {
      rawValues_ = nullptr;
    }
  }

  T valueAt(vector_size_t index) const override { return rawValues_[index]; }

  void copy(const BaseVector *source, vector_size_t sourceIndex, vector_size_t targetIndex) override {
    auto *srcVec = static_cast<const FlatVector<T> *>(source);
    const_cast<T *>(rawValues_)[targetIndex] = srcVec->valueAt(sourceIndex);

    if constexpr (std::is_same_v<T, StringView>) {
      // For demo simplicity, we assume string data is either inline or
      // guaranteed to outlive In full implementation, we would copy external
      // data to stringBuffers_
    }
  }

  const BufferPtr &values() const { return values_; }
  const T *rawValues() const { return rawValues_; }
  T *mutableRawValues() { return const_cast<T *>(rawValues_); }

  // Add string buffer support
  void addStringBuffer(BufferPtr buffer) { stringBuffers_.push_back(buffer); }

private:
  BufferPtr values_;
  const T *rawValues_;
  std::vector<BufferPtr> stringBuffers_;
};

} // namespace facebook::velox
