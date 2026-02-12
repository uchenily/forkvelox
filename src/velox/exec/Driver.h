#pragma once
#include "velox/exec/Operator.h"
#include "velox/exec/TaskStats.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <unordered_set>
#include <vector>

namespace facebook::velox::exec {

enum class BlockingReason {
  kNotBlocked,
  kWaitForSplit,
  kWaitForExchange,
  kWaitForJoin,
  kYield,
  kCancelled,
  kTerminated,
};

class Driver {
public:
  Driver(std::vector<std::shared_ptr<Operator>> operators)
      : operators_(std::move(operators)), stats_(operators_.size()) {
    for (size_t i = 0; i < operators_.size(); ++i) {
      const auto &op = operators_[i];
      stats_[i].planNodeId = op->planNode()->id();
      stats_[i].operatorType = operatorType(op);
    }
  }

  void setCancelCheck(std::function<bool()> cancelCheck) { cancelCheck_ = std::move(cancelCheck); }

  void setYieldCheck(std::function<bool()> yieldCheck) { yieldCheck_ = std::move(yieldCheck); }

  bool shouldStop() const { return cancelCheck_ && cancelCheck_(); }

  const std::vector<OperatorStats> &stats() const { return stats_; }

  BlockingReason run(std::vector<RowVectorPtr> &results) {
    std::cout << "[Driver] Starting execution pipeline." << std::endl;
    bool progress = true;
    while (progress) {
      progress = false;
      if (shouldStop()) {
        return BlockingReason::kCancelled;
      }
      if (yieldCheck_ && yieldCheck_()) {
        return BlockingReason::kYield;
      }

      auto &source = operators_.front();
      std::cout << "[Driver] Pulling from source " << source->planNode()->toString() << std::endl;
      auto batch = timedGetOutput(0, source);
      if (batch) {
        std::cout << "[Driver] Source produced " << batch->size() << std::endl;
        progress = true;
        for (size_t i = 1; i < operators_.size(); ++i) {
          timedAddInput(i, operators_[i], batch);
          batch = timedGetOutput(i, operators_[i]);
          if (!batch)
            break;
        }
        if (batch) {
          results.push_back(batch);
        }
      } else {
        if (!source->isFinished()) {
        } else {
          std::cout << "[Driver] Source finished." << std::endl;
          // Propagate finish
          for (size_t i = 1; i < operators_.size(); ++i) {
            timedNoMoreInput(i, operators_[i]);
          }

          // Now flush pipeline
          progress = true; // Retry loop to flush buffers
          while (progress) {
            progress = false;
            if (shouldStop()) {
              return BlockingReason::kCancelled;
            }
            if (yieldCheck_ && yieldCheck_()) {
              return BlockingReason::kYield;
            }
            for (size_t i = 1; i < operators_.size(); ++i) {
              auto batch = timedGetOutput(i, operators_[i]);
              if (batch) {
                std::cout << "[Driver] Flushed batch from " << operators_[i]->planNode()->toString() << std::endl;
                if (i == operators_.size() - 1) {
                  results.push_back(batch);
                } else {
                  timedAddInput(i + 1, operators_[i + 1], batch);
                }
                progress = true;
              }
            }
          }
          break; // Done
        }
      }
    }
    std::cout << "[Driver] Execution finished." << std::endl;
    return BlockingReason::kNotBlocked;
  }

private:
  static uint64_t estimateVectorBytes(const VectorPtr &vector) {
    if (!vector) {
      return 0;
    }

    switch (vector->type()->kind()) {
    case TypeKind::BIGINT:
      return vector->size() * sizeof(int64_t);
    case TypeKind::INTEGER:
      return vector->size() * sizeof(int32_t);
    case TypeKind::VARCHAR: {
      uint64_t bytes = vector->size() * sizeof(StringView);
      auto strings = std::dynamic_pointer_cast<SimpleVector<StringView>>(vector);
      if (strings) {
        for (vector_size_t row = 0; row < vector->size(); ++row) {
          if (!vector->isNullAt(row)) {
            bytes += strings->valueAt(row).size();
          }
        }
      }
      return bytes;
    }
    case TypeKind::ROW: {
      auto row = std::dynamic_pointer_cast<RowVector>(vector);
      if (!row) {
        return 0;
      }
      uint64_t bytes = 0;
      for (size_t i = 0; i < row->childrenSize(); ++i) {
        bytes += estimateVectorBytes(row->childAt(i));
      }
      return bytes;
    }
    case TypeKind::ARRAY: {
      auto array = std::dynamic_pointer_cast<ArrayVector>(vector);
      if (!array) {
        return 0;
      }
      return vector->size() * (sizeof(int32_t) + sizeof(vector_size_t)) + estimateVectorBytes(array->elements());
    }
    default:
      return 0;
    }
  }

  static uint64_t estimateRowVectorBytes(const RowVectorPtr &rowVector) { return estimateVectorBytes(rowVector); }

  static std::string operatorType(const std::shared_ptr<Operator> &op) {
    if (std::dynamic_pointer_cast<HashBuildOperator>(op)) {
      return "HashBuild";
    }
    if (std::dynamic_pointer_cast<HashProbeOperator>(op)) {
      return "HashProbe";
    }
    if (std::dynamic_pointer_cast<LocalExchangeSinkOperator>(op)) {
      return "LocalExchangeSink";
    }
    if (std::dynamic_pointer_cast<LocalExchangeSourceOperator>(op)) {
      return "LocalExchangeSource";
    }
    return op->planNode() ? op->planNode()->toString() : std::string("Unknown");
  }

  RowVectorPtr timedGetOutput(size_t operatorIndex, const std::shared_ptr<Operator> &op) {
    const auto start = std::chrono::steady_clock::now();
    auto batch = op->getOutput();
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start)
                             .count();

    auto &stats = stats_[operatorIndex];
    stats.getOutputCpuNanos += elapsed;
    stats.cpuNanos += elapsed;
    stats.touched = true;
    stats.numDrivers = 1;
    if (batch) {
      stats.outputRows += batch->size();
      stats.outputVectors += 1;
      const auto batchBytes = estimateRowVectorBytes(batch);
      stats.outputBytes += batchBytes;
      if (stats.operatorType == "FileScan" || stats.operatorType == "TableScan") {
        stats.inputRows += batch->size();
        stats.inputVectors += 1;
        stats.inputBytes += batchBytes;
        stats.rawInputRows += batch->size();
        stats.rawInputBytes += batchBytes;
      }
      stats.peakMemoryBytes =
          std::max<uint64_t>(stats.peakMemoryBytes, static_cast<uint64_t>(batch->pool()->currentBytes()));
    }
    return batch;
  }

  void timedAddInput(size_t operatorIndex, const std::shared_ptr<Operator> &op, const RowVectorPtr &input) {
    const auto start = std::chrono::steady_clock::now();
    op->addInput(input);
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start)
                             .count();

    auto &stats = stats_[operatorIndex];
    stats.addInputCpuNanos += elapsed;
    stats.cpuNanos += elapsed;
    stats.touched = true;
    stats.numDrivers = 1;
    if (input) {
      stats.inputRows += input->size();
      stats.inputVectors += 1;
      stats.inputBytes += estimateRowVectorBytes(input);
      stats.peakMemoryBytes =
          std::max<uint64_t>(stats.peakMemoryBytes, static_cast<uint64_t>(input->pool()->currentBytes()));
    }
  }

  void timedNoMoreInput(size_t operatorIndex, const std::shared_ptr<Operator> &op) {
    const auto start = std::chrono::steady_clock::now();
    op->noMoreInput();
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start)
                             .count();

    auto &stats = stats_[operatorIndex];
    stats.finishCpuNanos += elapsed;
    stats.cpuNanos += elapsed;
    stats.touched = true;
    stats.numDrivers = 1;
  }

  std::vector<std::shared_ptr<Operator>> operators_;
  std::vector<OperatorStats> stats_;
  std::function<bool()> cancelCheck_;
  std::function<bool()> yieldCheck_;
};

struct DriverFactory {
  std::vector<core::PlanNodePtr> planNodes;
  OperatorSupplier operatorSupplier;
  core::PlanNodePtr consumerNode;
  bool inputDriver{false};
  bool outputPipeline{false};
  size_t numDrivers{1};
  std::unordered_set<core::PlanNodeId> probeJoinIds;

  std::shared_ptr<Driver> createDriver(
      core::ExecCtx *execCtx,
      const std::function<std::shared_ptr<Operator>(const core::PlanNodePtr &, core::ExecCtx *)> &makeOperator) const {
    std::vector<std::shared_ptr<Operator>> ops;
    ops.reserve(planNodes.size() + (operatorSupplier ? 1 : 0));
    for (const auto &node : planNodes) {
      ops.push_back(makeOperator(node, execCtx));
    }
    if (operatorSupplier) {
      ops.push_back(operatorSupplier(execCtx));
    }
    return std::make_shared<Driver>(std::move(ops));
  }
};

} // namespace facebook::velox::exec
