#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <cstring>
#include "velox/common/memory/MemoryAllocator.h"

namespace facebook::velox::memory {

class MemoryPool : public std::enable_shared_from_this<MemoryPool> {
public:
    MemoryPool(const std::string& name, std::shared_ptr<MemoryPool> parent = nullptr) 
        : name_(name), parent_(parent), currentBytes_(0) {}

    virtual ~MemoryPool() = default;

    const std::string& name() const { return name_; }

    void* allocate(int64_t size) {
        reserve(size);
        try {
            return MemoryAllocator::getInstance()->allocateBytes(size);
        } catch (...) {
            release(size);
            throw;
        }
    }

    void free(void* p, int64_t size) {
        if (p) {
            MemoryAllocator::getInstance()->freeBytes(p, size);
            release(size);
        }
    }

    // Reallocate is common in Vectors
    void* reallocate(void* p, int64_t oldSize, int64_t newSize) {
        if (oldSize == newSize) return p;
        // Naive implementation
        void* newP = allocate(newSize);
        if (p) {
            std::memcpy(newP, p, std::min(oldSize, newSize));
            free(p, oldSize);
        }
        return newP;
    }

    int64_t currentBytes() const { return currentBytes_; }
    
    std::shared_ptr<MemoryPool> parent() const { return parent_; }
    
    std::shared_ptr<MemoryPool> addLeafChild(const std::string& name) {
         return std::make_shared<MemoryPool>(name, shared_from_this());
    }

    std::shared_ptr<MemoryPool> addAggregateChild(const std::string& name) {
         return std::make_shared<MemoryPool>(name, shared_from_this());
    }

private:
    void reserve(int64_t size) {
        currentBytes_ += size;
        if (parent_) parent_->reserve(size);
    }
    
    void release(int64_t size) {
        currentBytes_ -= size;
        if (parent_) parent_->release(size);
    }

    std::string name_;
    std::shared_ptr<MemoryPool> parent_;
    std::atomic<int64_t> currentBytes_;
};
}
