#include "velox/exec/Task.h"

#include <algorithm>
#include <atomic>
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
    ops.push_back(std::make_shared<HashJoinOperator>(node, ctx));
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

  auto runPlanWithDrivers = [&](const core::PlanNodePtr& plan) {
    SourceStateMap sourceStates;
    collectSourceStates(plan, queryCtx_->pool(), sourceStates);

    const size_t driverCount = std::max<size_t>(1, maxDrivers_);
    std::vector<RowVectorPtr> results;
    std::mutex resultsMutex;

    auto runDriver = [&]() {
      core::ExecCtx execCtx(queryCtx_->pool(), queryCtx_.get());
      std::vector<std::shared_ptr<Operator>> ops;
      buildPipeline(plan, ops, &execCtx, sourceStates);
      std::reverse(ops.begin(), ops.end());

      for (auto& op : ops) {
        if (auto joinOp = std::dynamic_pointer_cast<HashJoinOperator>(op)) {
          auto joinNode = std::dynamic_pointer_cast<const core::HashJoinNode>(
              joinOp->planNode());
          SourceStateMap buildStates;
          collectSourceStates(joinNode->sources()[1], execCtx.pool(), buildStates);
          std::vector<std::shared_ptr<Operator>> buildOps;
          buildPipeline(joinNode->sources()[1], buildOps, &execCtx, buildStates);
          std::reverse(buildOps.begin(), buildOps.end());
          ::facebook::velox::exec::Driver buildDriver(buildOps);
          joinOp->setBuildSide(buildDriver.run());
        }
      }

      ::facebook::velox::exec::Driver driver(ops);
      auto batches = driver.run();
      if (!batches.empty()) {
        std::lock_guard<std::mutex> lock(resultsMutex);
        results.insert(results.end(), batches.begin(), batches.end());
      }
    };

    if (mode_ == ExecutionMode::kParallel && driverCount > 1) {
      std::vector<std::thread> threads;
      threads.reserve(driverCount);
      for (size_t i = 0; i < driverCount; ++i) {
        threads.emplace_back(runDriver);
      }
      for (auto& thread : threads) {
        thread.join();
      }
    } else {
      for (size_t i = 0; i < driverCount; ++i) {
        runDriver();
      }
    }

    return results;
  };

  auto runBlockingWithValues = [&](const core::PlanNodePtr& blockingNode,
                                   const std::vector<RowVectorPtr>& batches) {
    auto valuesNode =
        std::make_shared<core::ValuesNode>("final_values", batches);
    std::vector<std::shared_ptr<Operator>> ops;
    ops.push_back(std::make_shared<ValuesOperator>(valuesNode));

    if (std::dynamic_pointer_cast<const core::OrderByNode>(blockingNode)) {
      ops.push_back(std::make_shared<OrderByOperator>(blockingNode));
    } else if (std::dynamic_pointer_cast<const core::TopNNode>(blockingNode)) {
      ops.push_back(std::make_shared<TopNOperator>(blockingNode));
    } else if (std::dynamic_pointer_cast<const core::AggregationNode>(blockingNode)) {
      core::ExecCtx execCtx(queryCtx_->pool(), queryCtx_.get());
      ops.push_back(std::make_shared<AggregationOperator>(blockingNode, &execCtx));
    } else if (std::dynamic_pointer_cast<const core::TableWriteNode>(blockingNode)) {
      ops.push_back(std::make_shared<TableWriteOperator>(blockingNode));
    } else {
      VELOX_FAIL("Unsupported blocking operator");
    }

    ::facebook::velox::exec::Driver driver(ops);
    return driver.run();
  };

  if (auto orderBy =
          std::dynamic_pointer_cast<const core::OrderByNode>(plan_)) {
    auto upstream = orderBy->sources()[0];
    auto partial = runPlanWithDrivers(upstream);
    return runBlockingWithValues(plan_, partial);
  }

  if (auto topN = std::dynamic_pointer_cast<const core::TopNNode>(plan_)) {
    auto upstream = topN->sources()[0];
    auto partial = runPlanWithDrivers(upstream);
    return runBlockingWithValues(plan_, partial);
  }

  if (auto agg = std::dynamic_pointer_cast<const core::AggregationNode>(plan_)) {
    auto upstream = agg->sources()[0];
    auto partial = runPlanWithDrivers(upstream);
    return runBlockingWithValues(plan_, partial);
  }

  if (auto write = std::dynamic_pointer_cast<const core::TableWriteNode>(plan_)) {
    auto upstream = write->sources()[0];
    auto partial = runPlanWithDrivers(upstream);
    return runBlockingWithValues(plan_, partial);
  }

  if (auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(plan_)) {
    auto probePlan = join->sources()[0];
    auto buildPlan = join->sources()[1];
    auto probeBatches = runPlanWithDrivers(probePlan);

    core::ExecCtx execCtx(queryCtx_->pool(), queryCtx_.get());
    SourceStateMap buildStates;
    collectSourceStates(buildPlan, execCtx.pool(), buildStates);
    std::vector<std::shared_ptr<Operator>> buildOps;
    buildPipeline(buildPlan, buildOps, &execCtx, buildStates);
    std::reverse(buildOps.begin(), buildOps.end());
    ::facebook::velox::exec::Driver buildDriver(buildOps);
    auto buildResults = buildDriver.run();

    auto valuesNode =
        std::make_shared<core::ValuesNode>("final_values", probeBatches);
    std::vector<std::shared_ptr<Operator>> ops;
    ops.push_back(std::make_shared<ValuesOperator>(valuesNode));
    auto joinOp = std::make_shared<HashJoinOperator>(plan_, &execCtx);
    joinOp->setBuildSide(buildResults);
    ops.push_back(joinOp);
    ::facebook::velox::exec::Driver driver(ops);
    return driver.run();
  }

  return runPlanWithDrivers(plan_);
}

} // namespace facebook::velox::exec
