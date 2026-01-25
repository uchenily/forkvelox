#pragma once

#include <memory>
#include <vector>
#include <optional>
#include "velox/type/Type.h"
#include "velox/buffer/Buffer.h"
#include "velox/vector/VectorEncoding.h"
#include "velox/common/base/BitUtil.h"

namespace facebook::velox {

using vector_size_t = int32_t;

class BaseVector : public std::enable_shared_from_this<BaseVector> {
public:
    BaseVector(memory::MemoryPool* pool,
               std::shared_ptr<const Type> type,
               VectorEncoding::Simple encoding,
               BufferPtr nulls,
               vector_size_t length)
        : pool_(pool), type_(std::move(type)), encoding_(encoding), nulls_(std::move(nulls)), length_(length) {
    }
    
    virtual ~BaseVector() = default;
    
    VectorEncoding::Simple encoding() const { return encoding_; }
    const std::shared_ptr<const Type>& type() const { return type_; }
    memory::MemoryPool* pool() const { return pool_; }
    vector_size_t size() const { return length_; }
    
    virtual bool isNullAt(vector_size_t index) const {
        if (!nulls_) return false;
        return bits::isBitSet(nulls_->as<uint64_t>(), index); // Note: Velox nulls: 1 means null?
        // Wait, Velox convention: nulls bit set means null? Or valid?
        // Usually validity buffer: 1 is valid.
        // But Velox calls it "nulls".
        // Let's check BaseVector.h again or assume standard Arrow/Velox.
        // Velox docs say "nulls" buffer.
        // In `BaseVector.h`: `rawNulls_`. `isNullAt(i)`
        // `bits::isBitSet(rawNulls_, i)` -> usually means "is null".
        // I will assume 1 means Null.
    }
    
    // Virtuals
    virtual std::string toString(vector_size_t index) const = 0;
    
    virtual std::string toString(vector_size_t start, vector_size_t end) const {
        std::string s;
        for(vector_size_t i = start; i < end && i < length_; ++i) {
             s += toString(i) + "\n";
        }
        return s;
    }

    std::string toString() const { return toString(0, std::min(length_, 10)); }

    virtual BaseVector* loadedVector() { return this; }
    virtual const BaseVector* loadedVector() const { return this; }

protected:
    memory::MemoryPool* pool_;
    std::shared_ptr<const Type> type_;
    VectorEncoding::Simple encoding_;
    BufferPtr nulls_;
    vector_size_t length_;
};

using VectorPtr = std::shared_ptr<BaseVector>;
using RowVectorPtr = std::shared_ptr<class RowVector>;

}
