#pragma once

#include "vector/BaseVector.h"
#include "type/StringView.h"
#include "buffer/Buffer.h"
#include <vector>

namespace facebook::velox {

template <typename T>
class SimpleVector : public BaseVector {
public:
    SimpleVector(memory::MemoryPool* pool, TypePtr type, VectorEncoding encoding, 
                 BufferPtr nulls, vector_size_t length, BufferPtr values)
        : BaseVector(pool, std::move(type), encoding, std::move(nulls), length), 
          values_(std::move(values)) {
          if (values_) {
              rawValues_ = values_->asMutable<T>();
          } else {
              rawValues_ = nullptr;
          }
    }

    virtual T valueAt(vector_size_t idx) const {
        return rawValues_[idx];
    }
    
    const T* rawValues() const { return rawValues_; }
    T* mutableRawValues() { return rawValues_; }

protected:
    BufferPtr values_;
    T* rawValues_;
};

template <typename T>
class FlatVector : public SimpleVector<T> {
public:
    FlatVector(memory::MemoryPool* pool, TypePtr type, BufferPtr nulls, 
               vector_size_t length, BufferPtr values, 
               std::vector<BufferPtr> stringBuffers = {})
        : SimpleVector<T>(pool, std::move(type), VectorEncoding::FLAT, std::move(nulls), length, std::move(values)),
          stringBuffers_(std::move(stringBuffers)) {}

    using BaseVector::toString;
    std::string toString(vector_size_t start, vector_size_t end) const override {
        std::stringstream ss;
        ss << "[";
        for (vector_size_t i = start; i < end; ++i) {
             if (i > start) ss << ", ";
             if (this->isNullAt(i)) {
                 ss << "null";
             } else {
                 ss << this->valueAt(i);
             }
        }
        ss << "]";
        return ss.str();
    }
    
    // For StringView, we might need to add buffers
    void addStringBuffer(BufferPtr buffer) {
        stringBuffers_.push_back(std::move(buffer));
    }

private:
    std::vector<BufferPtr> stringBuffers_;
};

// Helper for generic T
template <typename T>
std::shared_ptr<FlatVector<T>> makeFlatVector(const std::vector<T>& data, memory::MemoryPool* pool = nullptr) {
    if (!pool) {
        // Fallback or error? For demo, we usually pass pool implicitly via ExecCtx but here we need it.
        // We'll rely on the caller or use a default instance for testing.
        pool = memory::MemoryManager::getInstance().addRootPool(); 
    }
    
    // Create Type
    TypePtr type;
    if constexpr (std::is_same_v<T, int32_t>) type = IntegerType::create();
    else if constexpr (std::is_same_v<T, int64_t>) type = BigIntType::create();
    else if constexpr (std::is_same_v<T, StringView>) type = VarcharType::create();
    else throw std::runtime_error("Unsupported type");

    uint64_t dataSize = data.size() * sizeof(T);
    BufferPtr values = AlignedBuffer::allocate(dataSize, pool);
    std::memcpy(values->asMutable<T>(), data.data(), dataSize);
    
    return std::make_shared<FlatVector<T>>(pool, type, nullptr, data.size(), values);
}

// Specialization/Overload for std::string -> StringView
inline std::shared_ptr<FlatVector<StringView>> makeFlatVector(const std::vector<std::string>& data, memory::MemoryPool* pool) {
    if (!pool) pool = memory::MemoryManager::getInstance().addRootPool();
    
    TypePtr type = VarcharType::create();
    uint64_t vecSize = data.size();
    
    // Allocate StringViews
    BufferPtr values = AlignedBuffer::allocate(vecSize * sizeof(StringView), pool);
    StringView* rawViews = values->asMutable<StringView>();
    
    // Allocate a single buffer for all string data (simplified)
    uint64_t totalStringBytes = 0;
    for (const auto& s : data) totalStringBytes += s.size();
    
    BufferPtr stringBuf = nullptr;
    char* stringBufPtr = nullptr;
    if (totalStringBytes > 0) {
        stringBuf = AlignedBuffer::allocate(totalStringBytes, pool);
        stringBufPtr = stringBuf->asMutable<char>();
    }
    
    uint64_t offset = 0;
    for (size_t i = 0; i < vecSize; ++i) {
        if (data[i].size() <= StringView::kInlineSize) {
             rawViews[i] = StringView(data[i].data(), data[i].size());
        } else {
             // Copy to buffer
             std::memcpy(stringBufPtr + offset, data[i].data(), data[i].size());
             rawViews[i] = StringView(stringBufPtr + offset, data[i].size());
             offset += data[i].size();
        }
    }
    
    std::vector<BufferPtr> stringBuffers;
    if (stringBuf) stringBuffers.push_back(stringBuf);
    
    return std::make_shared<FlatVector<StringView>>(pool, type, nullptr, vecSize, values, stringBuffers);
}

} // namespace facebook::velox
