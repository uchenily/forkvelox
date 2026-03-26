#include "velox/exec/Task.h"

#include <atomic>
#include <chrono>
#include <deque>
#include <stdexcept>

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/RowVectorFile.h"

namespace facebook::velox::exec {

namespace {

class SharedValuesSourceState final : public SourceState {
 public:
  explicit SharedValuesSourceState(std::vector<RowVectorPtr> values) : values_(std::move(values)) {}

  BlockingReason pendingReason(std::shared_ptr<async::AsyncEvent>* event) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return next_ >= values_.size();
  }

  RowVectorPtr next() override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (next_ >= values_.size()) {
      return nullptr;
    }
    return values_[next_++];
  }

 private:
  mutable std::mutex mutex_;
  std::vector<RowVectorPtr> values_;
  size_t next_{0};
};

class SharedSplitSourceState final : public SourceState {
 public:
  explicit SharedSplitSourceState(std::function<void()> onStateChange) : onStateChange_(std::move(onStateChange)) {}

  void addSplit(Split split) {
    std::vector<std::shared_ptr<async::AsyncEvent>> waiters;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      splits_.push_back(std::move(split));
      waiters.swap(waiters_);
    }
    for (auto& waiter : waiters) {
      waiter->notify();
    }
    notify();
  }

  void noMoreSplits() {
    std::vector<std::shared_ptr<async::AsyncEvent>> waiters;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      noMoreSplits_ = true;
      waiters.swap(waiters_);
    }
    for (auto& waiter : waiters) {
      waiter->notify();
    }
    notify();
  }

  BlockingReason pendingReason(std::shared_ptr<async::AsyncEvent>* event) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!splits_.empty() || noMoreSplits_) {
      return BlockingReason::kNotBlocked;
    }
    if (event != nullptr) {
      auto waiter = std::make_shared<async::AsyncEvent>();
      *event = waiter;
      waiters_.push_back(waiter);
    }
    return BlockingReason::kWaitForSplit;
  }

  bool isFinished() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return noMoreSplits_ && splits_.empty();
  }

  RowVectorPtr next() override {
    Split split("");
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (splits_.empty()) {
        return nullptr;
      }
      split = std::move(splits_.front());
      splits_.pop_front();
    }
    return dwio::common::RowVectorFile::read(pool_, split.path());
  }

  void setPool(memory::MemoryPool* pool) {
    pool_ = pool;
  }

 private:
  void notify() {
    if (onStateChange_) {
      onStateChange_();
    }
  }

  mutable std::mutex mutex_;
  std::deque<Split> splits_;
  bool noMoreSplits_{false};
  memory::MemoryPool* pool_{nullptr};
  std::function<void()> onStateChange_;
  std::vector<std::shared_ptr<async::AsyncEvent>> waiters_;
};

void collectJoinBridges(
    const core::PlanNodePtr& planNode,
    std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>>& bridges) {
  if (!planNode) {
    return;
  }
  for (const auto& source : planNode->sources()) {
    collectJoinBridges(source, bridges);
  }
  if (auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
    bridges.try_emplace(join->id(), std::make_shared<HashJoinBridge>());
  }
}

std::shared_ptr<async::AsyncEvent> collectAnyEvents(std::vector<std::shared_ptr<async::AsyncEvent>> events) {
  auto anyEvent = std::make_shared<async::AsyncEvent>();
  auto done = std::make_shared<std::atomic<bool>>(false);
  for (const auto& event : events) {
    event->subscribe([anyEvent, done]() mutable {
      bool expected = false;
      if (done->compare_exchange_strong(expected, true)) {
        anyEvent->notify();
      }
    });
  }
  return anyEvent;
}

} // namespace

const char* blockingReasonName(BlockingReason reason) {
  switch (reason) {
    case BlockingReason::kNotBlocked:
      return "NotBlocked";
    case BlockingReason::kWaitForSplit:
      return "WaitForSplit";
    case BlockingReason::kWaitForProducer:
      return "WaitForProducer";
    case BlockingReason::kWaitForJoinBuild:
      return "WaitForJoinBuild";
    case BlockingReason::kYield:
      return "Yield";
    case BlockingReason::kCancelled:
      return "Cancelled";
  }
  return "Unknown";
}

bool Task::DriverPendingState::pending(std::shared_ptr<async::AsyncEvent>* outEvent) const {
  if (!event) {
    return false;
  }
  if (event->isReady()) {
    return false;
  }
  if (outEvent != nullptr) {
    *outEvent = event;
  }
  return true;
}

void Task::DriverPendingState::set(std::shared_ptr<async::AsyncEvent> inEvent, BlockingReason inReason) {
  event = std::move(inEvent);
  reason = inReason;
}

void Task::DriverPendingState::clear() {
  event.reset();
  reason = BlockingReason::kNotBlocked;
}

Driver::Driver(
    std::vector<std::shared_ptr<Operator>> operators,
    std::shared_ptr<core::ExecCtx> execCtx)
    : operators_(std::move(operators)), execCtx_(std::move(execCtx)) {}

BlockingReason Driver::run(
    std::vector<RowVectorPtr>& results,
    std::shared_ptr<async::AsyncEvent>* event,
    bool stopAtFirstBatch) {
  size_t quantum = 0;
  while (true) {
    if (cancelCheck_ && cancelCheck_()) {
      return BlockingReason::kCancelled;
    }
    if (yieldCheck_ && yieldCheck_()) {
      return BlockingReason::kYield;
    }
    if (quantum++ >= 64) {
      return BlockingReason::kYield;
    }

    auto pumped = pump(operators_.size() - 1);
    if (pumped.reason != BlockingReason::kNotBlocked) {
      if (event != nullptr) {
        *event = lastBlockingEvent_;
      }
      return pumped.reason;
    }
    if (pumped.produced) {
      results.push_back(std::move(pumped.batch));
      if (stopAtFirstBatch) {
        return BlockingReason::kNotBlocked;
      }
      continue;
    }
    if (isFinished()) {
      return BlockingReason::kNotBlocked;
    }
    if (!pumped.progress) {
      return BlockingReason::kYield;
    }
  }
}

BlockingReason Driver::pendingReason() {
  if (!lastBlockedOperator_) {
    return BlockingReason::kNotBlocked;
  }
  std::shared_ptr<async::AsyncEvent> event;
  lastBlockingReason_ = lastBlockedOperator_->pendingReason(&event);
  lastBlockingEvent_ = std::move(event);
  if (lastBlockingReason_ == BlockingReason::kNotBlocked) {
    lastBlockedOperator_ = nullptr;
  }
  return lastBlockingReason_;
}

bool Driver::isFinished() const {
  return !operators_.empty() && operators_.back()->isFinished();
}

Driver::PumpResult Driver::pump(size_t operatorIndex) {
  auto& op = operators_[operatorIndex];

  std::shared_ptr<async::AsyncEvent> event;
  auto reason = op->pendingReason(&event);
  if (reason != BlockingReason::kNotBlocked) {
    lastBlockedOperator_ = op.get();
    lastBlockingReason_ = reason;
    lastBlockingEvent_ = std::move(event);
    return PumpResult{false, false, false, nullptr, reason};
  }

  if (auto output = op->getOutput()) {
    return PumpResult{true, true, false, std::move(output), BlockingReason::kNotBlocked};
  }
  if (op->isFinished()) {
    return PumpResult{true, false, true, nullptr, BlockingReason::kNotBlocked};
  }
  if (operatorIndex == 0) {
    return PumpResult{};
  }

  if (!op->needsInput()) {
    return PumpResult{};
  }

  auto upstream = pump(operatorIndex - 1);
  if (upstream.reason != BlockingReason::kNotBlocked) {
    return upstream;
  }
  if (upstream.produced) {
    op->addInput(std::move(upstream.batch));
    return PumpResult{true, false, false, nullptr, BlockingReason::kNotBlocked};
  }
  if (upstream.atEnd) {
    op->noMoreInput();
    return PumpResult{true, false, false, nullptr, BlockingReason::kNotBlocked};
  }
  return upstream;
}

std::shared_ptr<Task> Task::create(
    const std::string& taskId,
    const core::PlanNodePtr& plan,
    std::shared_ptr<core::QueryCtx> queryCtx,
    ExecutionMode mode) {
  return std::make_shared<Task>(taskId, plan, std::move(queryCtx), mode);
}

Task::Task(
    std::string taskId,
    core::PlanNodePtr plan,
    std::shared_ptr<core::QueryCtx> queryCtx,
    ExecutionMode mode)
    : taskId_(std::move(taskId)), plan_(std::move(plan)), queryCtx_(std::move(queryCtx)), mode_(mode) {}

void Task::addSplit(const core::PlanNodeId& planNodeId, exec::Split split) {
  prepare();
  auto it = sourceStates_.find(planNodeId);
  VELOX_CHECK(it != sourceStates_.end(), "Unknown split source plan node {}", planNodeId);
  auto state = std::dynamic_pointer_cast<SharedSplitSourceState>(it->second);
  VELOX_CHECK(state != nullptr, "Plan node {} does not accept splits", planNodeId);
  state->addSplit(std::move(split));
}

void Task::noMoreSplits(const core::PlanNodeId& planNodeId) {
  prepare();
  auto it = sourceStates_.find(planNodeId);
  VELOX_CHECK(it != sourceStates_.end(), "Unknown split source plan node {}", planNodeId);
  auto state = std::dynamic_pointer_cast<SharedSplitSourceState>(it->second);
  VELOX_CHECK(state != nullptr, "Plan node {} does not accept splits", planNodeId);
  state->noMoreSplits();
}

void Task::start() {
  prepare();
  VELOX_CHECK(mode_ == ExecutionMode::kParallel, "Task::start requires parallel execution mode");
  std::lock_guard<std::mutex> lock(mutex_);
  scheduleWorkers();
}

bool Task::supportSerialExecutionMode() const {
  return true;
}

RowVectorPtr Task::next() {
  return nextImpl(nullptr);
}

async::AsyncValue<RowVectorPtr>::Sender Task::nextAsync() {
  auto value = async::AsyncValue<RowVectorPtr>{};
  auto self = shared_from_this();
  auto runtime = queryCtx_->runtime();
  runtime->launch([self, value, runtime]() mutable {
    auto step = std::make_shared<std::function<void()>>();
    *step = [self, value, runtime, step]() mutable {
      try {
        std::shared_ptr<async::AsyncEvent> event;
        auto batch = self->nextImpl(&event);
        if (batch || !event) {
          value.setValue(std::move(batch));
          return;
        }
        event->subscribe([runtime, step]() mutable {
          runtime->launch(*step);
        });
      } catch (...) {
        value.setError(std::current_exception());
      }
    };
    (*step)();
  });
  return value.sender();
}

RowVectorPtr Task::nextImpl(std::shared_ptr<async::AsyncEvent>* event) {
  prepare();
  VELOX_CHECK(mode_ == ExecutionMode::kSerial, "Task::next requires serial execution mode");
  VELOX_CHECK(supportSerialExecutionMode(), "Task does not support serial execution mode");

  for (;;) {
    int runnableDrivers = 0;
    std::vector<std::shared_ptr<async::AsyncEvent>> pendingEvents;
    for (size_t i = 0; i < drivers_.size(); ++i) {
      auto& entry = drivers_[i];
      if (entry.finished) {
        continue;
      }

      std::shared_ptr<async::AsyncEvent> pendingEvent;
      if (driverPendingStates_[i].pending(&pendingEvent)) {
        pendingEvents.push_back(std::move(pendingEvent));
        continue;
      }
      driverPendingStates_[i].clear();
      ++runnableDrivers;

      std::vector<RowVectorPtr> localResults;
      auto reason = entry.driver->run(localResults, &pendingEvent, true);
      if (!localResults.empty()) {
        return localResults.front();
      }
      if (entry.driver->isFinished()) {
        entry.finished = true;
        ++finishedDrivers_;
        continue;
      }
      if (reason == BlockingReason::kYield) {
        continue;
      }
      if (reason == BlockingReason::kCancelled) {
        throw std::runtime_error("Task cancelled");
      }
      if (pendingEvent) {
        driverPendingStates_[i].set(pendingEvent, reason);
        pendingEvents.push_back(std::move(pendingEvent));
      }
    }

    if (allDriversFinishedLocked()) {
      return nullptr;
    }
    if (runnableDrivers == 0) {
      if (event != nullptr && !pendingEvents.empty()) {
        *event = collectAnyEvents(std::move(pendingEvents));
        return nullptr;
      }
      VELOX_FAIL("Cannot make progress in serial mode: no drivers are ready");
    }
  }
}

std::vector<RowVectorPtr> Task::run() {
  start();

  {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&]() {
      return failed_ || (allDriversFinishedLocked() && activeWorkers_ == 0);
    });
  }

  if (exception_) {
    std::rethrow_exception(exception_);
  }
  return results_;
}

void Task::prepare() {
  std::call_once(prepared_, [&]() {
    collectJoinBridges(plan_, joinBridges_);
    LocalPlanner::plan(plan_, &joinBridges_, &driverFactories_, maxDrivers_);
    buildSharedStates();
    instantiateDrivers();
  });
}

void Task::buildSharedStates() {
  auto onStateChange = [this]() { wakeSchedulers(); };

  for (auto& [_, bridge] : joinBridges_) {
    bridge->setOnStateChange(onStateChange);
  }

  for (const auto& factory : driverFactories_) {
    for (const auto& planNode : factory->planNodes) {
      if (auto values = std::dynamic_pointer_cast<const core::ValuesNode>(planNode)) {
        if (sourceStates_.count(planNode->id()) == 0) {
          sourceStates_[planNode->id()] = std::make_shared<SharedValuesSourceState>(values->values());
        }
        continue;
      }

      if (std::dynamic_pointer_cast<const core::FileScanNode>(planNode)) {
        if (sourceStates_.count(planNode->id()) == 0) {
          auto state = std::make_shared<SharedSplitSourceState>(onStateChange);
          state->setPool(queryCtx_->pool());
          sourceStates_[planNode->id()] = state;
        }
        continue;
      }

      if (auto partition = std::dynamic_pointer_cast<const core::LocalPartitionNode>(planNode)) {
        auto& queue = exchanges_[partition->exchangeId()];
        if (!queue) {
          queue = std::make_shared<LocalExchangeQueue>(partition->exchangeId());
          queue->setOnStateChange(onStateChange);
        }
      }
    }
  }
}

void Task::instantiateDrivers() {
  size_t pipelineId = 0;
  size_t driverId = 0;
  for (const auto& factory : driverFactories_) {
    const bool producerPipeline = !factory->planNodes.empty() &&
        std::dynamic_pointer_cast<const core::LocalPartitionNode>(factory->planNodes.back()) != nullptr;
    std::shared_ptr<HashJoinBridge> buildBridge;
    if (factory->operatorSupplier) {
      if (auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(factory->consumerNode)) {
        buildBridge = joinBridges_.at(join->id());
      }
    }

    for (size_t i = 0; i < factory->numDrivers; ++i) {
      if (producerPipeline) {
        auto partition = std::dynamic_pointer_cast<const core::LocalPartitionNode>(factory->planNodes.back());
        exchanges_.at(partition->exchangeId())->addProducer();
      }
      if (buildBridge) {
        buildBridge->addBuildDriver();
      }

      drivers_.push_back(DriverEntry{makeDriver(*factory, driverId++, pipelineId), false, false, false, false});
      enqueueDriverLocked(drivers_.size() - 1);
    }
    ++pipelineId;
  }
  driverPendingStates_.resize(drivers_.size());
}

std::shared_ptr<Driver> Task::makeDriver(
    const DriverFactory& factory,
    size_t /*driverId*/,
    size_t /*pipelineId*/) {
  auto execCtx = std::make_shared<core::ExecCtx>(queryCtx_->pool(), queryCtx_.get());
  std::vector<std::shared_ptr<Operator>> operators;
  operators.reserve(factory.planNodes.size() + (factory.operatorSupplier ? 1 : 0));

  for (const auto& planNode : factory.planNodes) {
    operators.push_back(makeOperator(planNode, execCtx.get()));
  }
  if (factory.operatorSupplier) {
    operators.push_back(factory.operatorSupplier(execCtx.get()));
  }
  return std::make_shared<Driver>(std::move(operators), std::move(execCtx));
}

std::shared_ptr<Operator> Task::makeOperator(const core::PlanNodePtr& planNode, core::ExecCtx* execCtx) {
  if (std::dynamic_pointer_cast<const core::ValuesNode>(planNode)) {
    return std::make_shared<ValuesOperator>(planNode, sourceStates_.at(planNode->id()));
  }
  if (std::dynamic_pointer_cast<const core::FileScanNode>(planNode)) {
    return std::make_shared<FileScanOperator>(planNode, execCtx, sourceStates_.at(planNode->id()));
  }
  if (std::dynamic_pointer_cast<const core::TableWriteNode>(planNode)) {
    return std::make_shared<TableWriteOperator>(planNode);
  }
  if (std::dynamic_pointer_cast<const core::FilterNode>(planNode)) {
    return std::make_shared<FilterOperator>(planNode, execCtx);
  }
  if (std::dynamic_pointer_cast<const core::AggregationNode>(planNode)) {
    return std::make_shared<AggregationOperator>(planNode, execCtx);
  }
  if (std::dynamic_pointer_cast<const core::OrderByNode>(planNode)) {
    return std::make_shared<OrderByOperator>(planNode);
  }
  if (std::dynamic_pointer_cast<const core::TopNNode>(planNode)) {
    return std::make_shared<TopNOperator>(planNode);
  }
  if (auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
    return std::make_shared<HashProbeOperator>(planNode, execCtx, joinBridges_.at(join->id()));
  }
  if (auto partition = std::dynamic_pointer_cast<const core::LocalPartitionNode>(planNode)) {
    return std::make_shared<LocalExchangeSinkOperator>(planNode, exchanges_.at(partition->exchangeId()));
  }
  if (auto merge = std::dynamic_pointer_cast<const core::LocalMergeNode>(planNode)) {
    return std::make_shared<LocalExchangeSourceOperator>(planNode, exchanges_.at(merge->exchangeId()));
  }
  return std::make_shared<PassThroughOperator>(planNode);
}

void Task::workerLoop() {
  while (true) {
    size_t driverIndex = 0;
    std::shared_ptr<async::AsyncEvent> pendingEventToSubscribe;
    uint64_t pendingSequenceToSubscribe = 0;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [&]() { return failed_ || allDriversFinishedLocked() || !runnable_.empty(); });

      if (failed_ || allDriversFinishedLocked()) {
        if (activeWorkers_ > 0) {
          --activeWorkers_;
        }
        cv_.notify_all();
        return;
      }

      std::uniform_int_distribution<size_t> dist(0, runnable_.size() - 1);
      const size_t pick = dist(random_);
      driverIndex = runnable_[pick];
      runnable_[pick] = runnable_.back();
      runnable_.pop_back();
      auto& entry = drivers_[driverIndex];
      entry.enqueued = false;
      entry.running = true;
    }

    std::vector<RowVectorPtr> localResults;
    auto reason = BlockingReason::kNotBlocked;
    std::shared_ptr<async::AsyncEvent> event;
    try {
      auto start = std::chrono::steady_clock::now();
      drivers_[driverIndex].driver->setYieldCheck([start]() {
        return std::chrono::steady_clock::now() - start > std::chrono::milliseconds(2);
      });
      reason = drivers_[driverIndex].driver->run(localResults, &event);
    } catch (...) {
      std::lock_guard<std::mutex> lock(mutex_);
      exception_ = std::current_exception();
      failed_ = true;
      if (activeWorkers_ > 0) {
        --activeWorkers_;
      }
      cv_.notify_all();
      return;
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      for (auto& batch : localResults) {
        results_.push_back(std::move(batch));
      }

      auto& entry = drivers_[driverIndex];
      entry.running = false;
      driverPendingStates_[driverIndex].clear();
      if (entry.driver->isFinished()) {
        entry.finished = true;
        entry.pending = false;
        ++finishedDrivers_;
      } else if (reason == BlockingReason::kYield) {
        enqueueDriverLocked(driverIndex);
      } else if (reason == BlockingReason::kCancelled) {
        exception_ = std::make_exception_ptr(std::runtime_error("Task cancelled"));
        failed_ = true;
      } else {
        entry.pending = true;
        driverPendingStates_[driverIndex].set(event, reason);
        pendingSequenceToSubscribe = ++entry.pendingSequence;
        if (event) {
          pendingEventToSubscribe = event;
        }
      }
      cv_.notify_all();
    }

    if (pendingEventToSubscribe) {
      auto self = shared_from_this();
      pendingEventToSubscribe->subscribe([self, driverIndex, pendingSequenceToSubscribe]() mutable {
        self->resumePendingDriver(driverIndex, pendingSequenceToSubscribe);
      });
    }
  }
}

void Task::enqueueDriverLocked(size_t driverIndex) {
  auto& entry = drivers_[driverIndex];
  if (entry.finished || entry.running || entry.enqueued) {
    return;
  }
  entry.enqueued = true;
  runnable_.push_back(driverIndex);
}

void Task::scheduleWorkers() {
  if (workersScheduled_) {
    return;
  }
  workersScheduled_ = true;

  const size_t workers = mode_ == ExecutionMode::kSerial
      ? 1
      : std::max<size_t>(1, std::min<size_t>(
                               std::thread::hardware_concurrency() == 0 ? 4 : std::thread::hardware_concurrency(),
                               drivers_.size()));

  activeWorkers_ = workers;
  auto self = shared_from_this();
  auto runtime = queryCtx_->runtime();
  VELOX_CHECK(runtime != nullptr, "Task requires an execution runtime");
  for (size_t i = 0; i < workers; ++i) {
    runtime->launch([self]() { self->workerLoop(); });
  }
}

void Task::wakeSchedulers() {
  std::lock_guard<std::mutex> lock(mutex_);
  cv_.notify_all();
}

void Task::resumePendingDriver(size_t driverIndex, uint64_t pendingSequence) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (failed_ || allDriversFinishedLocked()) {
    return;
  }
  auto& entry = drivers_[driverIndex];
  if (entry.finished || !entry.pending || entry.pendingSequence != pendingSequence) {
    return;
  }
  driverPendingStates_[driverIndex].clear();
  entry.pending = false;
  enqueueDriverLocked(driverIndex);
  cv_.notify_all();
}

bool Task::allDriversFinishedLocked() const {
  return finishedDrivers_ == drivers_.size();
}

} // namespace facebook::velox::exec
