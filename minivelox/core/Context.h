#pragma once

#include "common/memory/Memory.h"
#include <memory>

namespace facebook::velox::core {

class QueryCtx {
public:
     static std::shared_ptr<QueryCtx> create(std::shared_ptr<void> executor = nullptr) {
         return std::make_shared<QueryCtx>();
     }
     
     // Simplified: QueryCtx owns the root pool for the query
     QueryCtx() {
         pool_ = memory::MemoryManager::getInstance().addRootPool("query_pool");
     }
     
     memory::MemoryPool* pool() const { return pool_; }

private:
     memory::MemoryPool* pool_;
};

class ExecCtx {
public:
    ExecCtx(memory::MemoryPool* pool, QueryCtx* queryCtx) : pool_(pool), queryCtx_(queryCtx) {}
    memory::MemoryPool* pool() const { return pool_; }
    QueryCtx* queryCtx() const { return queryCtx_; }
private:
    memory::MemoryPool* pool_;
    QueryCtx* queryCtx_;
};

} // namespace facebook::velox::core
