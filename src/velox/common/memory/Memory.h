#pragma once

#include "velox/common/memory/MemoryPool.h"
#include "velox/common/memory/MemoryAllocator.h"

namespace facebook::velox::memory {

class MemoryManager {
public:
    struct Options {
        int64_t capacity = 0; 
    };
    static void initialize(const Options& options);
    static MemoryManager* getInstance();
    
    std::shared_ptr<MemoryPool> addLeafPool(const std::string& name = "default") {
        return std::make_shared<MemoryPool>(name);
    }
};

inline void initializeMemoryManager(const MemoryManager::Options& options) {
    MemoryManager::initialize(options);
}

inline std::shared_ptr<MemoryPool> defaultMemoryPool() {
    return MemoryManager::getInstance()->addLeafPool();
}

}
