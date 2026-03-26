#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <random>
#include <unordered_map>
#include <vector>

#include "velox/common/async/Async.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Driver.h"
#include "velox/exec/LocalPlanner.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Split.h"
#include "velox/exec/TaskStats.h"

namespace facebook::velox::exec {

class LocalExchangeQueue;
class HashJoinBridge;

class Task : public std::enable_shared_from_this<Task> {
 public:
  enum class ExecutionMode {
    kSerial,
    kParallel,
  };

  static std::shared_ptr<Task> create(
      const std::string& taskId,
      const core::PlanNodePtr& plan,
      std::shared_ptr<core::QueryCtx> queryCtx,
      ExecutionMode mode);

  Task(
      std::string taskId,
      core::PlanNodePtr plan,
      std::shared_ptr<core::QueryCtx> queryCtx,
      ExecutionMode mode);

  void setMaxDrivers(size_t maxDrivers) {
    maxDrivers_ = std::max<size_t>(1, maxDrivers);
  }

  void addSplit(const core::PlanNodeId& planNodeId, exec::Split split);
  void noMoreSplits(const core::PlanNodeId& planNodeId);

  void start();
  bool supportSerialExecutionMode() const;
  RowVectorPtr next(std::shared_ptr<async::AsyncEvent>* event = nullptr);
  std::vector<RowVectorPtr> run();
  const TaskStats& stats() const {
    return stats_;
  }

 private:
  struct DriverBlockingState {
    std::shared_ptr<async::AsyncEvent> event;
    BlockingReason reason{BlockingReason::kNotBlocked};

    bool blocked(std::shared_ptr<async::AsyncEvent>* outEvent = nullptr) const;
    void set(std::shared_ptr<async::AsyncEvent> inEvent, BlockingReason inReason);
    void clear();
  };

  struct DriverEntry {
    std::shared_ptr<Driver> driver;
    bool finished{false};
    bool blocked{false};
    bool enqueued{false};
    bool running{false};
    uint64_t blockedSequence{0};
  };

  void prepare();
  void buildSharedStates();
  void instantiateDrivers();
  std::shared_ptr<Driver> makeDriver(
      const DriverFactory& factory,
      size_t driverId,
      size_t pipelineId);
  std::shared_ptr<Operator> makeOperator(
      const core::PlanNodePtr& planNode,
      core::ExecCtx* execCtx);
  void workerLoop();
  void enqueueDriverLocked(size_t driverIndex);
  void scheduleWorkers();
  void wakeSchedulers();
  void resumeDriverFromFuture(size_t driverIndex, uint64_t blockedSequence);
  bool allDriversFinishedLocked() const;

  std::string taskId_;
  core::PlanNodePtr plan_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
  ExecutionMode mode_;
  size_t maxDrivers_{1};

  std::once_flag prepared_;
  std::vector<std::unique_ptr<DriverFactory>> driverFactories_;
  std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>> joinBridges_;
  std::unordered_map<std::string, std::shared_ptr<LocalExchangeQueue>> exchanges_;
  std::unordered_map<core::PlanNodeId, std::shared_ptr<SourceState>> sourceStates_;

  std::vector<DriverEntry> drivers_;
  std::vector<DriverBlockingState> driverBlockingStates_;
  std::vector<size_t> runnable_;
  std::vector<RowVectorPtr> results_;
  TaskStats stats_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool cancelled_{false};
  bool failed_{false};
  bool workersScheduled_{false};
  std::exception_ptr exception_;
  size_t activeWorkers_{0};
  size_t finishedDrivers_{0};
  std::mt19937 random_{std::random_device{}()};
};

} // namespace facebook::velox::exec
