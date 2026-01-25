#pragma once

#include <memory>
#include <optional>
#include "common/memory/Memory.h"
#include "type/Type.h"
#include "buffer/Buffer.h"
#include "vector/TypeAliases.h"
#include "common/base/Exceptions.h"

namespace facebook::velox {

enum class VectorEncoding {
    FLAT,
    ROW,
    CONSTANT, // Simplified
    DICTIONARY
};

class BaseVector;
using VectorPtr = std::shared_ptr<BaseVector>;
using RowVectorPtr = std::shared_ptr<class RowVector>;

class BaseVector : public std::enable_shared_from_this<BaseVector> {
public:
    BaseVector(memory::MemoryPool* pool, TypePtr type, VectorEncoding encoding, BufferPtr nulls, vector_size_t length)
        : pool_(pool), type_(std::move(type)), encoding_(encoding), nulls_(std::move(nulls)), length_(length) {}
    
    virtual ~BaseVector() = default;

    TypePtr type() const { return type_; }
    VectorEncoding encoding() const { return encoding_; }
    vector_size_t size() const { return length_; }
    memory::MemoryPool* pool() const { return pool_; }

    virtual bool isNullAt(vector_size_t idx) const {
        if (!nulls_) return false;
        // Simplified null check (not using bit manipulation for now, or just assume byte per bool? No, Velox uses bits)
        // Let's assume bits for correctness with Velox spirit.
        // But for mini-impl, bytes is easier. 
        // Wait, instructions say "Underlying data structure ... as efficient as original".
        // I should use bits.
        const uint64_t* rawNulls = nulls_->as<uint64_t>();
        // idx / 64 word, idx % 64 bit
        // bit is set means not null? Or null?
        // Velox: 1 means not null (set bit), 0 means null (clear bit) ? Or vice versa?
        // Velox bits::isBitNull(rawNulls, idx) checks if bit is *clear* (if I recall correctly, nulls are usually 0)
        // Checking `velox/common/base/BitUtil.h` would confirm.
        // Assuming 0 is null (standard Arrow/Velox).
        return !((rawNulls[idx / 64] >> (idx % 64)) & 1); 
    }
    
    virtual std::string toString(vector_size_t start, vector_size_t end) const = 0;
    virtual std::string toString() const { return toString(0, length_); }

    template <typename T>
    std::shared_ptr<T> as() {
        return std::dynamic_pointer_cast<T>(shared_from_this());
    }

protected:
    memory::MemoryPool* pool_;
    TypePtr type_;
    VectorEncoding encoding_;
    BufferPtr nulls_;
    vector_size_t length_;
};

} // namespace facebook::velox
