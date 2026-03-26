#pragma once
#include "velox/common/memory/Memory.h"
#include "velox/core/ExecutionRuntime.h"
#include "velox/core/QueryConfig.h"
#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

namespace facebook::velox::core {

class QueryCtx : public std::enable_shared_from_this<QueryCtx> {
public:
  static std::shared_ptr<QueryCtx> create(std::shared_ptr<ExecutionRuntime> runtime = nullptr) {
    return std::make_shared<QueryCtx>(std::move(runtime));
  }

  explicit QueryCtx(std::shared_ptr<ExecutionRuntime> runtime = nullptr) : runtime_(std::move(runtime)) {
    pool_ = memory::MemoryManager::getInstance()->addLeafPool("query_pool");
    if (!runtime_) {
      runtime_ = std::make_shared<ExecutionRuntime>(std::max(1u, std::thread::hardware_concurrency()));
    }
  }

  memory::MemoryPool *pool() { return pool_.get(); }
  std::shared_ptr<ExecutionRuntime> runtime() const { return runtime_; }

private:
  std::shared_ptr<ExecutionRuntime> runtime_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

} // namespace facebook::velox::core
