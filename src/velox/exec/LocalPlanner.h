#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

class LocalPlanner {
 public:
  static void plan(
      const core::PlanNodePtr& plan,
      const std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>>*
          bridges,
      std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
      size_t maxDrivers);
};

} // namespace facebook::velox::exec
