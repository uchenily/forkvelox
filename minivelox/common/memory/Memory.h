#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <atomic>
#include <string>
#include <iostream>
#include <cstring>
#include <vector>
#include "common/base/Exceptions.h"

namespace facebook::velox::memory {

class MemoryPool {
 public:
  MemoryPool(const std::string& name, int64_t capacity) 
      : name_(name), capacity_(capacity), currentBytes_(0) {}
  
  virtual ~MemoryPool() {
      if (currentBytes_ != 0) {
          std::cerr << "MemoryPool " << name_ << " leaked " << currentBytes_ << " bytes." << std::endl;
      }
  }

  void* allocate(int64_t size) {
      if (currentBytes_ + size > capacity_) {
          throw VeloxRuntimeError("Memory limit exceeded");
      }
      // 64-byte alignment is standard for AVX512 etc.
      void* ptr = std::aligned_alloc(64, size); 
      if (!ptr) {
           throw std::bad_alloc();
      }
      currentBytes_ += size;
      return ptr;
  }

  void free(void* p, int64_t size) {
      if (p) {
          std::free(p);
          currentBytes_ -= size;
      }
  }

  void* reallocate(void* p, int64_t oldSize, int64_t newSize) {
      if (newSize <= oldSize) return p;
      void* newPtr = allocate(newSize);
      if (p) {
          std::memcpy(newPtr, p, oldSize);
          free(p, oldSize);
      }
      return newPtr;
  }

  int64_t currentBytes() const { return currentBytes_; }
  const std::string& name() const { return name_; }

 private:
  std::string name_;
  int64_t capacity_;
  std::atomic<int64_t> currentBytes_;
};

// Simplified MemoryManager
class MemoryManager {
public:
    static MemoryManager& getInstance() {
        static MemoryManager instance;
        return instance;
    }

    MemoryPool* addRootPool(const std::string& name = "default", int64_t capacity = 4L * 1024 * 1024 * 1024) { // 4GB default
        // Simple ownership for demo
        auto pool = std::make_unique<MemoryPool>(name, capacity);
        MemoryPool* ptr = pool.get();
        pools_.push_back(std::move(pool));
        return ptr;
    }

private:
    std::vector<std::unique_ptr<MemoryPool>> pools_;
};

} // namespace facebook::velox::memory
