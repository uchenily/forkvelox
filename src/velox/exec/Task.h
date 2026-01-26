#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "velox/core/ExecCtx.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Split.h"
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
  void addSplit(const core::PlanNodeId& id, exec::Split split);
  void noMoreSplits(const core::PlanNodeId& id);

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
  std::unordered_map<core::PlanNodeId, std::vector<exec::Split>> splits_;
  std::unordered_set<core::PlanNodeId> noMoreSplits_;
};

} // namespace facebook::velox::exec
