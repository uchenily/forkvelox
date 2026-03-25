#include "velox/exec/Task.h"

#include <chrono>
#include <deque>
#include <stdexcept>
#include <thread>

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/RowVectorFile.h"

namespace facebook::velox::exec {

namespace {

class SharedValuesSourceState final : public SourceState {
 public:
  explicit SharedValuesSourceState(std::vector<RowVectorPtr> values) : values_(std::move(values)) {}

  BlockingReason isBlocked(ContinueFuture* future) override {
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
    std::vector<std::shared_ptr<ContinuePromise>> waiters;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      splits_.push_back(std::move(split));
      waiters.swap(waiters_);
    }
    for (auto& waiter : waiters) {
      waiter->set_value();
    }
    notify();
  }

  void noMoreSplits() {
    std::vector<std::shared_ptr<ContinuePromise>> waiters;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      noMoreSplits_ = true;
      waiters.swap(waiters_);
    }
    for (auto& waiter : waiters) {
      waiter->set_value();
    }
    notify();
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!splits_.empty() || noMoreSplits_) {
      return BlockingReason::kNotBlocked;
    }
    if (future != nullptr) {
      auto waiter = std::make_shared<ContinuePromise>();
      *future = waiter->get_future().share();
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
  std::vector<std::shared_ptr<ContinuePromise>> waiters_;
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

Driver::Driver(
    std::vector<std::shared_ptr<Operator>> operators,
    std::shared_ptr<core::ExecCtx> execCtx)
    : operators_(std::move(operators)), execCtx_(std::move(execCtx)) {}

BlockingReason Driver::run(std::vector<RowVectorPtr>& results, ContinueFuture* future) {
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
      if (future != nullptr) {
        *future = lastBlockingFuture_;
      }
      return pumped.reason;
    }
    if (pumped.produced) {
      results.push_back(std::move(pumped.batch));
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

BlockingReason Driver::isBlocked() {
  if (!lastBlockedOperator_) {
    return BlockingReason::kNotBlocked;
  }
  ContinueFuture future;
  lastBlockingReason_ = lastBlockedOperator_->isBlocked(&future);
  lastBlockingFuture_ = future;
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

  ContinueFuture future;
  auto reason = op->isBlocked(&future);
  if (reason != BlockingReason::kNotBlocked) {
    lastBlockedOperator_ = op.get();
    lastBlockingReason_ = reason;
    lastBlockingFuture_ = future;
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

std::vector<RowVectorPtr> Task::run() {
  prepare();

  const size_t workers = mode_ == ExecutionMode::kSerial
      ? 1
      : std::max<size_t>(1, std::min<size_t>(
                               std::thread::hardware_concurrency() == 0 ? 4 : std::thread::hardware_concurrency(),
                               drivers_.size()));

  std::vector<std::thread> threads;
  threads.reserve(workers > 0 ? workers - 1 : 0);
  for (size_t i = 1; i < workers; ++i) {
    threads.emplace_back([this]() { workerLoop(); });
  }
  workerLoop();
  for (auto& thread : threads) {
    thread.join();
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

    for (size_t i = 0; i < factory->numDrivers; ++i) {
      if (producerPipeline) {
        auto partition = std::dynamic_pointer_cast<const core::LocalPartitionNode>(factory->planNodes.back());
        exchanges_.at(partition->exchangeId())->addProducer();
      }

      drivers_.push_back(DriverEntry{makeDriver(*factory, driverId++, pipelineId), false, false, false, false});
      enqueueDriverLocked(drivers_.size() - 1);
    }
    ++pipelineId;
  }
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
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [&]() { return failed_ || allDriversFinishedLocked() || !runnable_.empty(); });

      if (failed_ || allDriversFinishedLocked()) {
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
    ContinueFuture future;
    try {
      auto start = std::chrono::steady_clock::now();
      drivers_[driverIndex].driver->setYieldCheck([start]() {
        return std::chrono::steady_clock::now() - start > std::chrono::milliseconds(2);
      });
      reason = drivers_[driverIndex].driver->run(localResults, &future);
    } catch (...) {
      std::lock_guard<std::mutex> lock(mutex_);
      exception_ = std::current_exception();
      failed_ = true;
      cv_.notify_all();
      return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& batch : localResults) {
      results_.push_back(std::move(batch));
    }

    auto& entry = drivers_[driverIndex];
    entry.running = false;
    if (entry.driver->isFinished()) {
      entry.finished = true;
      entry.blocked = false;
      ++finishedDrivers_;
    } else if (reason == BlockingReason::kYield) {
      enqueueDriverLocked(driverIndex);
    } else if (reason == BlockingReason::kCancelled) {
      exception_ = std::make_exception_ptr(std::runtime_error("Task cancelled"));
      failed_ = true;
    } else {
      entry.blocked = true;
      const auto blockedSequence = ++entry.blockedSequence;
      if (future.valid()) {
        auto self = shared_from_this();
        auto* executor = queryCtx_->executor();
        std::thread([self, driverIndex, blockedSequence, future, executor]() mutable {
          future.wait();
          if (executor != nullptr) {
            executor->add([self, driverIndex, blockedSequence]() {
              self->resumeDriverFromFuture(driverIndex, blockedSequence);
            });
          } else {
            self->resumeDriverFromFuture(driverIndex, blockedSequence);
          }
        }).detach();
      }
    }
    cv_.notify_all();
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

void Task::wakeSchedulers() {
  std::lock_guard<std::mutex> lock(mutex_);
  cv_.notify_all();
}

void Task::resumeDriverFromFuture(size_t driverIndex, uint64_t blockedSequence) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (failed_ || allDriversFinishedLocked()) {
    return;
  }
  auto& entry = drivers_[driverIndex];
  if (entry.finished || !entry.blocked || entry.blockedSequence != blockedSequence) {
    return;
  }
  entry.blocked = false;
  enqueueDriverLocked(driverIndex);
  cv_.notify_all();
}

bool Task::allDriversFinishedLocked() const {
  return finishedDrivers_ == drivers_.size();
}

} // namespace facebook::velox::exec
