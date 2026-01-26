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
#include "velox/exec/Operator.h"
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

using SourceStateMap =
    std::unordered_map<core::PlanNodeId, std::shared_ptr<SourceState>>;

void collectSourceStates(
    const core::PlanNodePtr& node,
    memory::MemoryPool* pool,
    SourceStateMap& states) {
  if (auto values = std::dynamic_pointer_cast<const core::ValuesNode>(node)) {
    states.emplace(values->id(),
                   std::make_shared<ValuesSourceState>(values->values()));
  } else if (auto fileScan =
                 std::dynamic_pointer_cast<const core::FileScanNode>(node)) {
    states.emplace(
        fileScan->id(),
        std::make_shared<FileScanSourceState>(
            fileScan->path(), fileScan->outputType(), pool, 1024));
  }

  for (const auto& source : node->sources()) {
    collectSourceStates(source, pool, states);
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

std::vector<RowVectorPtr> Task::run() {
  VELOX_CHECK(queryCtx_ != nullptr, "QueryCtx must not be null");

  auto isBlockingNode = [](const core::PlanNodePtr& node) {
    return std::dynamic_pointer_cast<const core::OrderByNode>(node) ||
        std::dynamic_pointer_cast<const core::TopNNode>(node) ||
        std::dynamic_pointer_cast<const core::AggregationNode>(node) ||
        std::dynamic_pointer_cast<const core::TableWriteNode>(node);
  };

  auto linearizeChain = [](core::PlanNodePtr root) {
    std::vector<core::PlanNodePtr> chain;
    auto current = root;
    while (current) {
      chain.push_back(current);
      auto sources = current->sources();
      if (sources.empty()) {
        break;
      }
      current = sources[0];
    }
    std::reverse(chain.begin(), chain.end());
    return chain;
  };

  auto splitPipelines = [&](const std::vector<core::PlanNodePtr>& chain) {
    std::vector<std::vector<core::PlanNodePtr>> pipelines;
    std::vector<core::PlanNodePtr> current;
    for (const auto& node : chain) {
      if (!current.empty() && isBlockingNode(node)) {
        pipelines.push_back(current);
        current.clear();
      }
      current.push_back(node);
    }
    if (!current.empty()) {
      pipelines.push_back(current);
    }
    return pipelines;
  };

  using HashJoinBridgeMap =
      std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>>;

  auto makeOperatorForNode =
      [&](const core::PlanNodePtr& node,
          core::ExecCtx* ctx,
          const SourceStateMap& states,
          const HashJoinBridgeMap& bridges) -> std::shared_ptr<Operator> {
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
    return std::static_pointer_cast<Operator>(
        std::make_shared<PassThroughOperator>(node));
  };

  auto runPipelines =
      [&](const core::PlanNodePtr& plan,
          const HashJoinBridgeMap& bridges,
          const std::function<std::shared_ptr<Operator>(core::ExecCtx*)>& tailOpFactory) {
    SourceStateMap sourceStates;
    collectSourceStates(plan, queryCtx_->pool(), sourceStates);

    auto chain = linearizeChain(plan);
    auto pipelineNodes = splitPipelines(chain);

    struct Pipeline {
      std::vector<core::PlanNodePtr> nodes;
      std::shared_ptr<LocalExchangeQueue> inputQueue;
      std::shared_ptr<LocalExchangeQueue> outputQueue;
      size_t numDrivers{1};
    };

    std::vector<Pipeline> pipelines;
    pipelines.reserve(pipelineNodes.size());
    for (size_t i = 0; i < pipelineNodes.size(); ++i) {
      Pipeline pipeline{
          pipelineNodes[i], nullptr, nullptr, std::max<size_t>(1, maxDrivers_)};
      for (const auto& node : pipeline.nodes) {
        if (isBlockingNode(node)) {
          pipeline.numDrivers = 1;
          break;
        }
      }
      pipelines.push_back(std::move(pipeline));
    }
    for (size_t i = 0; i + 1 < pipelines.size(); ++i) {
      auto queue =
          std::make_shared<LocalExchangeQueue>(pipelines[i].numDrivers);
      pipelines[i].outputQueue = queue;
      pipelines[i + 1].inputQueue = queue;
    }

    std::vector<RowVectorPtr> results;
    std::mutex resultsMutex;
    std::vector<std::thread> threads;

    for (size_t pipelineId = 0; pipelineId < pipelines.size(); ++pipelineId) {
      auto& pipeline = pipelines[pipelineId];
      for (size_t driverId = 0; driverId < pipeline.numDrivers; ++driverId) {
        threads.emplace_back([&, pipelineId]() {
          core::ExecCtx execCtx(queryCtx_->pool(), queryCtx_.get());
          std::vector<std::shared_ptr<Operator>> ops;

          if (pipeline.inputQueue) {
            auto exchangeNode =
                std::make_shared<core::LocalExchangeNode>("exchange_source");
            ops.push_back(std::make_shared<LocalExchangeSourceOperator>(
                exchangeNode, pipeline.inputQueue));
          }

          for (const auto& node : pipeline.nodes) {
            ops.push_back(
                makeOperatorForNode(node, &execCtx, sourceStates, bridges));
          }

          if (tailOpFactory && pipelineId == pipelines.size() - 1) {
            ops.push_back(tailOpFactory(&execCtx));
          }

          if (pipeline.outputQueue) {
            auto exchangeNode =
                std::make_shared<core::LocalExchangeNode>("exchange_sink");
            ops.push_back(std::make_shared<LocalExchangeSinkOperator>(
                exchangeNode, pipeline.outputQueue));
          }

          ::facebook::velox::exec::Driver driver(ops);
          auto batches = driver.run();
          if (!pipeline.outputQueue && !batches.empty()) {
            std::lock_guard<std::mutex> lock(resultsMutex);
            results.insert(results.end(), batches.begin(), batches.end());
          }
        });
      }
    }

    for (auto& thread : threads) {
      thread.join();
    }

    return results;
  };

  HashJoinBridgeMap bridges;
  std::function<void(const core::PlanNodePtr&)> buildJoins =
      [&](const core::PlanNodePtr& node) {
        if (!node) {
          return;
        }
        for (const auto& source : node->sources()) {
          buildJoins(source);
        }
        auto join =
            std::dynamic_pointer_cast<const core::HashJoinNode>(node);
        if (!join) {
          return;
        }
        if (bridges.find(join->id()) != bridges.end()) {
          return;
        }
        auto bridge = std::make_shared<HashJoinBridge>();
        bridges.emplace(join->id(), bridge);
        auto buildNode = join->sources()[1];
        runPipelines(
            buildNode,
            bridges,
            [&](core::ExecCtx* execCtx) {
              return std::make_shared<HashBuildOperator>(node, bridge);
            });
      };

  buildJoins(plan_);

  return runPipelines(plan_, bridges, {});
}

} // namespace facebook::velox::exec
