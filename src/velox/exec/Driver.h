#pragma once
#include "velox/exec/Operator.h"
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
  Driver(std::vector<std::shared_ptr<Operator>> operators) : operators_(std::move(operators)) {}

  void setCancelCheck(std::function<bool()> cancelCheck) { cancelCheck_ = std::move(cancelCheck); }

  void setYieldCheck(std::function<bool()> yieldCheck) { yieldCheck_ = std::move(yieldCheck); }

  bool shouldStop() const { return cancelCheck_ && cancelCheck_(); }

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
      auto batch = source->getOutput();
      if (batch) {
        std::cout << "[Driver] Source produced " << batch->size() << std::endl;
        progress = true;
        for (size_t i = 1; i < operators_.size(); ++i) {
          operators_[i]->addInput(batch);
          batch = operators_[i]->getOutput();
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
            operators_[i]->noMoreInput();
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
              auto batch = operators_[i]->getOutput();
              if (batch) {
                std::cout << "[Driver] Flushed batch from " << operators_[i]->planNode()->toString() << std::endl;
                if (i == operators_.size() - 1)
                  results.push_back(batch);
                else
                  operators_[i + 1]->addInput(batch);
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
  std::vector<std::shared_ptr<Operator>> operators_;
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
