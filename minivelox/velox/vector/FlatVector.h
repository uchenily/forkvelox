#pragma once

#include "velox/vector/SimpleVector.h"
#include "velox/buffer/Buffer.h"

namespace facebook::velox {

template <typename T>
class FlatVector : public SimpleVector<T> {
public:
    FlatVector(memory::MemoryPool* pool,
               std::shared_ptr<const Type> type,
               BufferPtr nulls,
               vector_size_t length,
               BufferPtr values,
               std::vector<BufferPtr> stringBuffers = {})
        : SimpleVector<T>(pool, type, VectorEncoding::Simple::FLAT, std::move(nulls), length),
          values_(std::move(values)),
          stringBuffers_(std::move(stringBuffers)) {
          
          if (values_) {
              rawValues_ = values_->as<T>();
          } else {
              rawValues_ = nullptr;
          }
    }
    
    T valueAt(vector_size_t index) const override {
        return rawValues_[index];
    }
    
    const BufferPtr& values() const { return values_; }
    const T* rawValues() const { return rawValues_; }
    T* mutableRawValues() { return const_cast<T*>(rawValues_); }

    // Add string buffer support
    void addStringBuffer(BufferPtr buffer) {
        stringBuffers_.push_back(buffer);
    }

private:
    BufferPtr values_;
    const T* rawValues_;
    std::vector<BufferPtr> stringBuffers_;
};

}
