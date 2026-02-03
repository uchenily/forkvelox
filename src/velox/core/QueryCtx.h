#pragma once
#include "folly/Executor.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/QueryConfig.h"
#include <memory>
#include <string>
#include <unordered_map>

namespace facebook::velox::core {

class QueryCtx : public std::enable_shared_from_this<QueryCtx> {
public:
  static std::shared_ptr<QueryCtx> create(folly::Executor *executor = nullptr) {
    return std::make_shared<QueryCtx>(executor);
  }

  QueryCtx(folly::Executor *executor = nullptr) : executor_(executor) {
    pool_ = memory::MemoryManager::getInstance()->addLeafPool("query_pool");
  }

  memory::MemoryPool *pool() { return pool_.get(); }
  folly::Executor *executor() const { return executor_; }

private:
  folly::Executor *executor_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

} // namespace facebook::velox::core
