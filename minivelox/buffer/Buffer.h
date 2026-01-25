#pragma once

#include <memory>
#include <cstdint>
#include "common/memory/Memory.h"
#include "vector/TypeAliases.h"
#include "common/base/Exceptions.h"

namespace facebook::velox {

class Buffer;
using BufferPtr = std::shared_ptr<Buffer>;

class Buffer {
public:
    Buffer(memory::MemoryPool* pool, uint64_t size) : pool_(pool), size_(size) {
        if (size_ > 0) {
            data_ = static_cast<uint8_t*>(pool_->allocate(size_));
        } else {
            data_ = nullptr;
        }
    }

    ~Buffer() {
        if (data_) {
            pool_->free(data_, size_);
        }
    }

    template <typename T>
    const T* as() const {
        return reinterpret_cast<const T*>(data_);
    }

    template <typename T>
    T* asMutable() {
        return reinterpret_cast<T*>(data_);
    }

    uint64_t size() const { return size_; }
    uint64_t capacity() const { return size_; } // Simplified

private:
    memory::MemoryPool* pool_;
    uint8_t* data_;
    uint64_t size_;
};

class AlignedBuffer {
public:
    static BufferPtr allocate(uint64_t size, memory::MemoryPool* pool) {
        return std::make_shared<Buffer>(pool, size);
    }
};

} // namespace facebook::velox
