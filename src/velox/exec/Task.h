#pragma once

#include <memory>
#include <string>
#include <vector>

#include "velox/core/ExecCtx.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

class Task {
public:
  enum class ExecutionMode { kSerial, kParallel };

  static std::shared_ptr<Task> create(
      std::string taskId,
      core::PlanNodePtr plan,
      std::shared_ptr<core::QueryCtx> queryCtx,
      ExecutionMode mode = ExecutionMode::kSerial);

  void setMaxDrivers(size_t count) { maxDrivers_ = count; }

  std::vector<RowVectorPtr> run();

private:
  Task(
      std::string taskId,
      core::PlanNodePtr plan,
      std::shared_ptr<core::QueryCtx> queryCtx,
      ExecutionMode mode);

  std::string taskId_;
  core::PlanNodePtr plan_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
  ExecutionMode mode_;
  size_t maxDrivers_{1};
};

} // namespace facebook::velox::exec
