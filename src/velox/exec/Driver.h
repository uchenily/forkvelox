#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "velox/common/async/Async.h"
#include "velox/core/ExecCtx.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/BlockingReason.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

using OperatorSupplier = std::function<std::shared_ptr<Operator>(core::ExecCtx*)>;

struct DriverFactory {
  std::vector<core::PlanNodePtr> planNodes;
  size_t numDrivers{1};
  bool inputDriver{false};
  OperatorSupplier operatorSupplier;
  core::PlanNodePtr consumerNode;
};

class Driver {
 public:
  Driver(
      std::vector<std::shared_ptr<Operator>> operators,
      std::shared_ptr<core::ExecCtx> execCtx = nullptr);

  BlockingReason run(
      std::vector<RowVectorPtr>& results,
      std::shared_ptr<async::AsyncEvent>* event = nullptr,
      bool stopAtFirstBatch = false);
  BlockingReason pendingReason();
  bool isFinished() const;

  void setYieldCheck(std::function<bool()> yieldCheck) {
    yieldCheck_ = std::move(yieldCheck);
  }

  void setCancelCheck(std::function<bool()> cancelCheck) {
    cancelCheck_ = std::move(cancelCheck);
  }

  const std::vector<std::shared_ptr<Operator>>& operators() const {
    return operators_;
  }

 private:
  struct PumpResult {
    bool progress{false};
    bool produced{false};
    bool atEnd{false};
    RowVectorPtr batch;
    BlockingReason reason{BlockingReason::kNotBlocked};
  };

  PumpResult pump(size_t operatorIndex);

  std::vector<std::shared_ptr<Operator>> operators_;
  std::shared_ptr<core::ExecCtx> execCtx_;
  std::function<bool()> yieldCheck_;
  std::function<bool()> cancelCheck_;
  Operator* lastBlockedOperator_{nullptr};
  BlockingReason lastBlockingReason_{BlockingReason::kNotBlocked};
  std::shared_ptr<async::AsyncEvent> lastBlockingEvent_;
};

} // namespace facebook::velox::exec
