#include "velox/common/memory/Memory.h"

namespace facebook::velox::memory {

static MemoryManager* g_memoryManager = nullptr;
static MemoryAllocator g_memoryAllocator;

MemoryAllocator* MemoryAllocator::getInstance() {
    return &g_memoryAllocator;
}

void MemoryManager::initialize(const Options& options) {
    if (!g_memoryManager) {
        g_memoryManager = new MemoryManager();
    }
}

MemoryManager* MemoryManager::getInstance() {
    if (!g_memoryManager) {
        initialize({});
    }
    return g_memoryManager;
}

}
