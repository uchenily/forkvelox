#pragma once

#include <atomic>
#include <latch>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "velox/core/ExecCtx.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Driver.h"
#include "velox/exec/TaskStats.h"
#include "velox/exec/Split.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

class Task {
public:
  enum class ExecutionMode { kSerial, kParallel };

  static std::shared_ptr<Task> create(std::string taskId, core::PlanNodePtr plan,
                                      std::shared_ptr<core::QueryCtx> queryCtx,
                                      ExecutionMode mode = ExecutionMode::kSerial);

  void setMaxDrivers(size_t count) { maxDrivers_ = count; }
  void addSplit(const core::PlanNodeId &id, exec::Split split);
  void noMoreSplits(const core::PlanNodeId &id);

  void requestCancel();
  bool isCancelled() const;
  void setError(std::string message);
  bool hasError() const;
  std::string errorMessage() const;
  bool shouldStop() const;

  std::vector<RowVectorPtr> run();
  TaskStats taskStats() const;
  std::string printPlanWithStats(bool includeCustomStats = false) const;

private:
  Task(std::string taskId, core::PlanNodePtr plan, std::shared_ptr<core::QueryCtx> queryCtx, ExecutionMode mode);

  void enqueue(folly::Executor *executor, std::shared_ptr<Driver> driver, std::shared_ptr<core::ExecCtx> execCtx,
               bool outputPipeline, size_t pipelineId, std::vector<RowVectorPtr> *results, std::mutex *resultsMutex,
               std::shared_ptr<std::latch> done);
  void addDriverStats(size_t pipelineId, const std::vector<OperatorStats> &stats);

  std::string taskId_;
  core::PlanNodePtr plan_; // tree根节点
  std::shared_ptr<core::QueryCtx> queryCtx_;
  ExecutionMode mode_;
  size_t maxDrivers_{1};
  std::unordered_map<core::PlanNodeId, std::vector<exec::Split>> splits_;
  std::unordered_set<core::PlanNodeId> noMoreSplits_;
  std::atomic<bool> cancelled_{false};
  mutable std::mutex errorMutex_;
  std::string errorMessage_;
  mutable std::mutex statsMutex_;
  TaskStats taskStats_;
};

} // namespace facebook::velox::exec
