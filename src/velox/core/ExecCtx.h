#pragma once
#include "velox/core/QueryCtx.h"

namespace facebook::velox::core {

class ExecCtx {
public:
  ExecCtx(memory::MemoryPool *pool, QueryCtx *queryCtx) : pool_(pool), queryCtx_(queryCtx) {}

  memory::MemoryPool *pool() const { return pool_; }
  QueryCtx *queryCtx() const { return queryCtx_; }

private:
  memory::MemoryPool *pool_;
  QueryCtx *queryCtx_;
};

} // namespace facebook::velox::core
