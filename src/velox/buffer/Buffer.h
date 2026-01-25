#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>
#include "velox/common/base/IntrusivePtr.h"
#include "velox/common/memory/Memory.h"

namespace facebook::velox {

class Buffer {
public:
    virtual ~Buffer() = default;
    
    void addRef() {
        refCount_.fetch_add(1);
    }
    
    void release() {
        if (refCount_.fetch_sub(1) == 1) {
            delete this;
        }
    }
    
    virtual const uint8_t* as_uint8_t() const = 0;
    virtual uint8_t* as_mutable_uint8_t() = 0;
    virtual uint64_t size() const = 0;
    virtual uint64_t capacity() const = 0;

    template<typename T>
    const T* as() const {
        return reinterpret_cast<const T*>(as_uint8_t());
    }

    template<typename T>
    T* asMutable() {
        return reinterpret_cast<T*>(as_mutable_uint8_t());
    }

protected:
    std::atomic<int32_t> refCount_{0};
};

using BufferPtr = IntrusivePtr<Buffer>;

class AlignedBuffer : public Buffer {
public:
    AlignedBuffer(memory::MemoryPool* pool, uint64_t size) 
        : pool_(pool), size_(size), capacity_(size) {
        if (pool_) {
            data_ = static_cast<uint8_t*>(pool_->allocate(capacity_));
        } else {
             // Fallback or error? Velox usually requires pool.
             data_ = static_cast<uint8_t*>(std::malloc(capacity_));
        }
    }
    
    ~AlignedBuffer() override {
        if (data_) {
            if (pool_) {
                pool_->free(data_, capacity_);
            } else {
                std::free(data_);
            }
        }
    }
    
    const uint8_t* as_uint8_t() const override { return data_; }
    uint8_t* as_mutable_uint8_t() override { return data_; }
    uint64_t size() const override { return size_; }
    uint64_t capacity() const override { return capacity_; }

    void setSize(uint64_t size) {
        VELOX_CHECK_LE(size, capacity_);
        size_ = size;
    }

    static BufferPtr allocate(uint64_t size, memory::MemoryPool* pool) {
        return BufferPtr(new AlignedBuffer(pool, size));
    }

private:
    memory::MemoryPool* pool_;
    uint8_t* data_;
    uint64_t size_;
    uint64_t capacity_;
};

}
