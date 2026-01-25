#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <new>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::memory {

class MemoryAllocator {
public:
    static MemoryAllocator* getInstance();
    
    void* allocateBytes(uint64_t bytes, uint16_t alignment = 64) {
        if (bytes == 0) return nullptr;
        // Ensure size is multiple of alignment for aligned_alloc
        if (alignment == 0) alignment = 64;
        uint64_t remainder = bytes % alignment;
        if (remainder != 0) {
            bytes += (alignment - remainder);
        }
        
        void* result = std::aligned_alloc(alignment, bytes);
        if (!result) throw std::bad_alloc();
        return result;
    }
    
    void freeBytes(void* p, uint64_t bytes) noexcept {
        std::free(p);
    }
};

}
