#include "velox/exec/Task.h"

#include <algorithm>
#include <atomic>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>

#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/RowVectorFile.h"
#include "velox/exec/Driver.h"
#include "velox/exec/LocalPlanner.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Split.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {
namespace {

class ValuesSourceState : public SourceState {
public:
  explicit ValuesSourceState(std::vector<RowVectorPtr> values)
      : values_(std::move(values)) {}

  RowVectorPtr next() override {
    auto index = next_.fetch_add(1);
    if (index >= values_.size()) {
      return nullptr;
    }
    return values_[index];
  }

private:
  std::vector<RowVectorPtr> values_;
  std::atomic<size_t> next_{0};
};

RowVectorPtr sliceRowVector(
    const RowVector& data,
    vector_size_t offset,
    vector_size_t length,
    memory::MemoryPool* pool) {
  auto rowType = asRowType(data.type());
  std::vector<VectorPtr> children;
  children.reserve(rowType->size());

  for (size_t col = 0; col < rowType->size(); ++col) {
    auto type = rowType->childAt(col);
    VectorPtr output;
    if (type->kind() == TypeKind::BIGINT) {
      output = std::make_shared<FlatVector<int64_t>>(
          pool,
          type,
          nullptr,
          length,
          AlignedBuffer::allocate(length * sizeof(int64_t), pool));
    } else if (type->kind() == TypeKind::INTEGER) {
      output = std::make_shared<FlatVector<int32_t>>(
          pool,
          type,
          nullptr,
          length,
          AlignedBuffer::allocate(length * sizeof(int32_t), pool));
    } else if (type->kind() == TypeKind::VARCHAR) {
      output = std::make_shared<FlatVector<StringView>>(
          pool,
          type,
          nullptr,
          length,
          AlignedBuffer::allocate(length * sizeof(StringView), pool));
    } else {
      VELOX_FAIL("Unsupported type for file scan slicing");
    }
    for (vector_size_t row = 0; row < length; ++row) {
      output->copy(data.childAt(col).get(), offset + row, row);
    }
    children.push_back(output);
  }

  return std::make_shared<RowVector>(
      pool, rowType, nullptr, length, std::move(children));
}

class FileScanSourceState : public SourceState {
public:
  FileScanSourceState(
      std::string path,
      RowTypePtr expectedType,
      memory::MemoryPool* pool,
      vector_size_t batchSize)
      : path_(std::move(path)),
        expectedType_(std::move(expectedType)),
        pool_(pool),
        batchSize_(batchSize) {}

  RowVectorPtr next() override {
    std::call_once(loadOnce_, [&]() { load(); });
    auto index = next_.fetch_add(1);
    if (index >= batches_.size()) {
      return nullptr;
    }
    return batches_[index];
  }

private:
  void load() {
    data_ = dwio::common::RowVectorFile::read(pool_, path_);
    if (!data_) {
      return;
    }
    if (expectedType_ && !expectedType_->equivalent(*data_->type())) {
      VELOX_FAIL("File schema does not match expected output type");
    }
    if (batchSize_ <= 0 || data_->size() <= batchSize_) {
      batches_.push_back(data_);
      return;
    }
    for (vector_size_t offset = 0; offset < data_->size();
         offset += batchSize_) {
      auto length =
          std::min(batchSize_, static_cast<vector_size_t>(data_->size() - offset));
      batches_.push_back(sliceRowVector(*data_, offset, length, pool_));
    }
  }

  std::string path_;
  RowTypePtr expectedType_;
  memory::MemoryPool* pool_;
  vector_size_t batchSize_;
  RowVectorPtr data_;
  std::vector<RowVectorPtr> batches_;
  std::atomic<size_t> next_{0};
  std::once_flag loadOnce_;
};

class FileSplitSourceState : public SourceState {
public:
  FileSplitSourceState(
      std::vector<exec::Split> splits,
      RowTypePtr expectedType,
      memory::MemoryPool* pool,
      vector_size_t batchSize)
      : splits_(std::move(splits)),
        expectedType_(std::move(expectedType)),
        pool_(pool),
        batchSize_(batchSize) {}

  RowVectorPtr next() override {
    std::call_once(loadOnce_, [&]() { load(); });
    auto index = next_.fetch_add(1);
    if (index >= batches_.size()) {
      return nullptr;
    }
    return batches_[index];
  }

private:
  void load() {
    for (const auto& split : splits_) {
      if (split.path().empty()) {
        continue;
      }
      auto data = dwio::common::RowVectorFile::read(pool_, split.path());
      if (!data) {
        continue;
      }
      if (expectedType_ && !expectedType_->equivalent(*data->type())) {
        VELOX_FAIL("File schema does not match expected output type");
      }
      if (batchSize_ <= 0 || data->size() <= batchSize_) {
        batches_.push_back(data);
        continue;
      }
      for (vector_size_t offset = 0; offset < data->size();
           offset += batchSize_) {
        auto length = std::min(
            batchSize_, static_cast<vector_size_t>(data->size() - offset));
        batches_.push_back(sliceRowVector(*data, offset, length, pool_));
      }
    }
  }

  std::vector<exec::Split> splits_;
  RowTypePtr expectedType_;
  memory::MemoryPool* pool_;
  vector_size_t batchSize_;
  std::vector<RowVectorPtr> batches_;
  std::atomic<size_t> next_{0};
  std::once_flag loadOnce_;
};

using SourceStateMap =
    std::unordered_map<core::PlanNodeId, std::shared_ptr<SourceState>>;

void collectSourceStates(
    const core::PlanNodePtr& node,
    memory::MemoryPool* pool,
    const std::unordered_map<core::PlanNodeId, std::vector<exec::Split>>& splits,
    SourceStateMap& states) {
  if (auto values = std::dynamic_pointer_cast<const core::ValuesNode>(node)) {
    states.emplace(values->id(),
                   std::make_shared<ValuesSourceState>(values->values()));
  } else if (auto fileScan =
                 std::dynamic_pointer_cast<const core::FileScanNode>(node)) {
    auto splitIt = splits.find(fileScan->id());
    if (splitIt != splits.end() && !splitIt->second.empty()) {
      states.emplace(
          fileScan->id(),
          std::make_shared<FileSplitSourceState>(
              splitIt->second, fileScan->outputType(), pool, 1024));
    } else {
      states.emplace(
          fileScan->id(),
          std::make_shared<FileScanSourceState>(
              fileScan->path(), fileScan->outputType(), pool, 1024));
    }
  }

  for (const auto& source : node->sources()) {
    collectSourceStates(source, pool, splits, states);
  }
}

void buildPipeline(
    core::PlanNodePtr node,
    std::vector<std::shared_ptr<Operator>>& ops,
    core::ExecCtx* ctx,
    const SourceStateMap& states) {
  if (auto values = std::dynamic_pointer_cast<const core::ValuesNode>(node)) {
    auto it = states.find(values->id());
    ops.push_back(std::make_shared<ValuesOperator>(
        node, it == states.end() ? nullptr : it->second));
  } else if (auto fileScan =
                 std::dynamic_pointer_cast<const core::FileScanNode>(node)) {
    auto it = states.find(fileScan->id());
    ops.push_back(std::make_shared<FileScanOperator>(
        node, ctx, it == states.end() ? nullptr : it->second));
  } else if (auto write =
                 std::dynamic_pointer_cast<const core::TableWriteNode>(node)) {
    ops.push_back(std::make_shared<TableWriteOperator>(node));
  } else if (auto filter =
                 std::dynamic_pointer_cast<const core::FilterNode>(node)) {
    ops.push_back(std::make_shared<FilterOperator>(node, ctx));
  } else if (auto agg =
                 std::dynamic_pointer_cast<const core::AggregationNode>(node)) {
    ops.push_back(std::make_shared<AggregationOperator>(node, ctx));
  } else if (auto orderBy =
                 std::dynamic_pointer_cast<const core::OrderByNode>(node)) {
    ops.push_back(std::make_shared<OrderByOperator>(node));
  } else if (auto topN =
                 std::dynamic_pointer_cast<const core::TopNNode>(node)) {
    ops.push_back(std::make_shared<TopNOperator>(node));
  } else if (auto join =
                 std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
    ops.push_back(std::make_shared<HashProbeOperator>(node, ctx, nullptr));
  } else if (auto scan =
                 std::dynamic_pointer_cast<const core::TableScanNode>(node)) {
    ops.push_back(std::make_shared<PassThroughOperator>(node));
  } else {
    ops.push_back(std::make_shared<PassThroughOperator>(node));
  }

  auto sources = node->sources();
  if (!sources.empty()) {
    buildPipeline(sources[0], ops, ctx, states);
  }
}

} // namespace

std::shared_ptr<Task> Task::create(
    std::string taskId,
    core::PlanNodePtr plan,
    std::shared_ptr<core::QueryCtx> queryCtx,
    ExecutionMode mode) {
  return std::shared_ptr<Task>(
      new Task(std::move(taskId), std::move(plan), std::move(queryCtx), mode));
}

Task::Task(
    std::string taskId,
    core::PlanNodePtr plan,
    std::shared_ptr<core::QueryCtx> queryCtx,
    ExecutionMode mode)
    : taskId_(std::move(taskId)),
      plan_(std::move(plan)),
      queryCtx_(std::move(queryCtx)),
      mode_(mode) {}

void Task::addSplit(const core::PlanNodeId& id, exec::Split split) {
  splits_[id].push_back(std::move(split));
}

void Task::noMoreSplits(const core::PlanNodeId& id) {
  noMoreSplits_.insert(id);
}

std::vector<RowVectorPtr> Task::run() {
  VELOX_CHECK(queryCtx_ != nullptr, "QueryCtx must not be null");

  using HashJoinBridgeMap =
      std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>>;
  using ExchangeQueueMap =
      std::unordered_map<std::string, std::shared_ptr<LocalExchangeQueue>>;

  auto makeOperatorForNode =
      [&](const core::PlanNodePtr& node,
          core::ExecCtx* ctx,
          const SourceStateMap& states,
          const HashJoinBridgeMap& bridges,
          const ExchangeQueueMap& exchanges) -> std::shared_ptr<Operator> {
    if (auto values = std::dynamic_pointer_cast<const core::ValuesNode>(node)) {
      auto it = states.find(values->id());
      return std::static_pointer_cast<Operator>(
          std::make_shared<ValuesOperator>(
              node, it == states.end() ? nullptr : it->second));
    }
    if (auto fileScan =
            std::dynamic_pointer_cast<const core::FileScanNode>(node)) {
      auto it = states.find(fileScan->id());
      return std::static_pointer_cast<Operator>(
          std::make_shared<FileScanOperator>(
              node, ctx, it == states.end() ? nullptr : it->second));
    }
    if (std::dynamic_pointer_cast<const core::TableWriteNode>(node)) {
      return std::static_pointer_cast<Operator>(
          std::make_shared<TableWriteOperator>(node));
    }
    if (std::dynamic_pointer_cast<const core::FilterNode>(node)) {
      return std::static_pointer_cast<Operator>(
          std::make_shared<FilterOperator>(node, ctx));
    }
    if (std::dynamic_pointer_cast<const core::AggregationNode>(node)) {
      return std::static_pointer_cast<Operator>(
          std::make_shared<AggregationOperator>(node, ctx));
    }
    if (std::dynamic_pointer_cast<const core::OrderByNode>(node)) {
      return std::static_pointer_cast<Operator>(
          std::make_shared<OrderByOperator>(node));
    }
    if (std::dynamic_pointer_cast<const core::TopNNode>(node)) {
      return std::static_pointer_cast<Operator>(
          std::make_shared<TopNOperator>(node));
    }
    if (std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
      auto it = bridges.find(node->id());
      if (it == bridges.end()) {
        VELOX_FAIL("Missing HashJoin bridge for plan node");
      }
      return std::static_pointer_cast<Operator>(
          std::make_shared<HashProbeOperator>(node, ctx, it->second));
    }
    if (auto localPartition =
            std::dynamic_pointer_cast<const core::LocalPartitionNode>(node)) {
      auto it = exchanges.find(localPartition->exchangeId());
      VELOX_CHECK(it != exchanges.end(), "Missing LocalExchange queue");
      return std::static_pointer_cast<Operator>(
          std::make_shared<LocalExchangeSinkOperator>(node, it->second));
    }
    if (auto localMerge =
            std::dynamic_pointer_cast<const core::LocalMergeNode>(node)) {
      auto it = exchanges.find(localMerge->exchangeId());
      VELOX_CHECK(it != exchanges.end(), "Missing LocalExchange queue");
      return std::static_pointer_cast<Operator>(
          std::make_shared<LocalExchangeSourceOperator>(node, it->second));
    }
    return std::static_pointer_cast<Operator>(
        std::make_shared<PassThroughOperator>(node));
  };

  HashJoinBridgeMap bridges;
  std::function<void(const core::PlanNodePtr&)> collectJoins =
      [&](const core::PlanNodePtr& node) {
        if (!node) {
          return;
        }
        for (const auto& source : node->sources()) {
          collectJoins(source);
        }
        if (auto join =
                std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
          if (bridges.find(join->id()) == bridges.end()) {
            bridges.emplace(join->id(), std::make_shared<HashJoinBridge>());
          }
        }
      };

  collectJoins(plan_);

  std::vector<std::unique_ptr<DriverFactory>> driverFactories;
  LocalPlanner::plan(plan_, &bridges, &driverFactories, maxDrivers_);

  for (auto& factory : driverFactories) {
    if (!factory->planNodes.empty() &&
        factory->planNodes.back()->id() == plan_->id()) {
      factory->outputPipeline = true;
    }
    for (const auto& node : factory->planNodes) {
      if (std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
        factory->probeJoinIds.insert(node->id());
      }
    }
  }

  std::cout << "[Task] Pipelines: " << driverFactories.size() << std::endl;
  auto opNameForNode = [](const core::PlanNodePtr& node, bool isBuild) {
    if (std::dynamic_pointer_cast<const core::ValuesNode>(node)) {
      return std::string("Values");
    }
    if (std::dynamic_pointer_cast<const core::FileScanNode>(node)) {
      return std::string("FileScan");
    }
    if (std::dynamic_pointer_cast<const core::TableWriteNode>(node)) {
      return std::string("TableWrite");
    }
    if (std::dynamic_pointer_cast<const core::FilterNode>(node)) {
      return std::string("Filter");
    }
    if (std::dynamic_pointer_cast<const core::AggregationNode>(node)) {
      return std::string("Aggregation");
    }
    if (std::dynamic_pointer_cast<const core::OrderByNode>(node)) {
      return std::string("OrderBy");
    }
    if (std::dynamic_pointer_cast<const core::TopNNode>(node)) {
      return std::string("TopN");
    }
    if (std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
      return isBuild ? std::string("HashBuild") : std::string("HashProbe");
    }
    if (std::dynamic_pointer_cast<const core::LocalPartitionNode>(node)) {
      return std::string("LocalExchangeSink");
    }
    if (std::dynamic_pointer_cast<const core::LocalMergeNode>(node)) {
      return std::string("LocalExchangeSource");
    }
    return node ? node->toString() : std::string("Unknown");
  };

  for (size_t i = 0; i < driverFactories.size(); ++i) {
    std::cout << "[Task] Pipeline " << i << " drivers=" << driverFactories[i]->numDrivers
              << (driverFactories[i]->outputPipeline ? " output" : "")
              << (driverFactories[i]->inputDriver ? " input" : "")
              << " operators: ";
    bool first = true;
    for (const auto& node : driverFactories[i]->planNodes) {
      if (!first) {
        std::cout << " -> ";
      }
      std::cout << opNameForNode(node, false);
      first = false;
    }
    if (driverFactories[i]->operatorSupplier &&
        driverFactories[i]->consumerNode) {
      if (!first) {
        std::cout << " -> ";
      }
      std::cout << opNameForNode(driverFactories[i]->consumerNode, true);
    }
    std::cout << std::endl;
  }

  std::unordered_map<std::string, size_t> exchangeProducers;
  std::unordered_map<std::string, size_t> exchangeConsumers;
  for (size_t i = 0; i < driverFactories.size(); ++i) {
    for (const auto& node : driverFactories[i]->planNodes) {
      if (auto localPartition =
              std::dynamic_pointer_cast<const core::LocalPartitionNode>(node)) {
        exchangeProducers.emplace(localPartition->exchangeId(), i);
      } else if (
          auto localMerge =
              std::dynamic_pointer_cast<const core::LocalMergeNode>(node)) {
        exchangeConsumers.emplace(localMerge->exchangeId(), i);
      }
    }
  }

  ExchangeQueueMap exchanges;
  for (const auto& [exchangeId, pipelineId] : exchangeProducers) {
    exchanges.emplace(
        exchangeId,
        std::make_shared<LocalExchangeQueue>(
            driverFactories[pipelineId]->numDrivers));
  }

  std::unordered_map<core::PlanNodeId, size_t> buildPipelines;
  for (size_t i = 0; i < driverFactories.size(); ++i) {
    const auto& factory = driverFactories[i];
    if (factory->operatorSupplier &&
        std::dynamic_pointer_cast<const core::HashJoinNode>(
            factory->consumerNode)) {
      buildPipelines.emplace(factory->consumerNode->id(), i);
    }
  }

  std::vector<std::vector<size_t>> dependents(driverFactories.size());
  std::vector<size_t> indegree(driverFactories.size(), 0);
  for (size_t i = 0; i < driverFactories.size(); ++i) {
    for (const auto& joinId : driverFactories[i]->probeJoinIds) {
      auto it = buildPipelines.find(joinId);
      VELOX_CHECK(it != buildPipelines.end(), "Missing build pipeline for join");
      dependents[it->second].push_back(i);
      indegree[i]++;
    }
  }
  for (const auto& [exchangeId, producerId] : exchangeProducers) {
    auto consumerIt = exchangeConsumers.find(exchangeId);
    if (consumerIt != exchangeConsumers.end()) {
      auto consumerId = consumerIt->second;
      dependents[producerId].push_back(consumerId);
      indegree[consumerId]++;
    }
  }

  SourceStateMap sourceStates;
  collectSourceStates(plan_, queryCtx_->pool(), splits_, sourceStates);

  std::vector<RowVectorPtr> results;
  std::mutex resultsMutex;

  std::vector<size_t> ready;
  ready.reserve(driverFactories.size());
  for (size_t i = 0; i < driverFactories.size(); ++i) {
    if (indegree[i] == 0) {
      ready.push_back(i);
    }
  }

  while (!ready.empty()) {
    auto pipelineId = ready.back();
    ready.pop_back();
    auto& factory = driverFactories[pipelineId];

    std::vector<std::thread> threads;
    for (size_t driverId = 0; driverId < factory->numDrivers; ++driverId) {
      threads.emplace_back([&, pipelineId]() {
        core::ExecCtx execCtx(queryCtx_->pool(), queryCtx_.get());
        auto driver = driverFactories[pipelineId]->createDriver(
            &execCtx,
            [&](const core::PlanNodePtr& node, core::ExecCtx* ctx) {
              return makeOperatorForNode(
                  node, ctx, sourceStates, bridges, exchanges);
            });
        auto batches = driver->run();
        if (driverFactories[pipelineId]->outputPipeline && !batches.empty()) {
          std::lock_guard<std::mutex> lock(resultsMutex);
          results.insert(results.end(), batches.begin(), batches.end());
        }
      });
    }

    for (auto& thread : threads) {
      thread.join();
    }

    for (auto dependent : dependents[pipelineId]) {
      if (--indegree[dependent] == 0) {
        ready.push_back(dependent);
      }
    }
  }

  return results;
}

} // namespace facebook::velox::exec
