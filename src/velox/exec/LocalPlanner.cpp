#include "velox/exec/LocalPlanner.h"

#include <limits>

#include "velox/common/base/Exceptions.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {
namespace detail {

bool mustStartNewPipeline(const core::PlanNodePtr &planNode, int32_t sourceId) {
  if (std::dynamic_pointer_cast<const core::LocalMergeNode>(planNode)) {
    return true;
  }
  return sourceId != 0;
}

OperatorSupplier
makeOperatorSupplier(const core::PlanNodePtr &planNode,
                     const std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>> *bridges) {
  if (auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
    VELOX_CHECK(bridges != nullptr, "Missing join bridges for HashJoin build");
    auto it = bridges->find(join->id());
    VELOX_CHECK(it != bridges->end(), "Missing HashJoin bridge for plan node");
    return [joinNode = planNode, bridge = it->second](core::ExecCtx *execCtx) {
      return std::make_shared<HashBuildOperator>(joinNode, bridge);
    };
  }
  return OperatorSupplier();
}

void plan(const core::PlanNodePtr &planNode, std::vector<core::PlanNodePtr> *currentPlanNodes,
          const core::PlanNodePtr &consumerNode, OperatorSupplier operatorSupplier,
          const std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>> *bridges,
          std::vector<std::unique_ptr<DriverFactory>> *driverFactories) {
  if (!currentPlanNodes) {
    auto driverFactory = std::make_unique<DriverFactory>();
    currentPlanNodes = &driverFactory->planNodes;
    driverFactory->operatorSupplier = std::move(operatorSupplier);
    driverFactory->consumerNode = consumerNode;
    driverFactories->push_back(std::move(driverFactory));
  }

  const auto &sources = planNode->sources();
  if (sources.empty()) {
    driverFactories->back()->inputDriver = true;
  } else {
    for (int32_t i = 0; i < static_cast<int32_t>(sources.size()); ++i) {
      plan(sources[i], mustStartNewPipeline(planNode, i) ? nullptr : currentPlanNodes, planNode,
           makeOperatorSupplier(planNode, bridges), bridges, driverFactories);
    }
  }

  currentPlanNodes->push_back(planNode);
}

size_t maxDrivers(const DriverFactory &driverFactory, size_t maxDriversHint) {
  size_t count = std::max<size_t>(1, maxDriversHint);
  for (const auto &node : driverFactory.planNodes) {
    if (std::dynamic_pointer_cast<const core::TopNNode>(node)) {
      return 1;
    }
    if (auto orderBy = std::dynamic_pointer_cast<const core::OrderByNode>(node)) {
      if (!orderBy->isPartial()) {
        return 1;
      }
    }
    if (auto agg = std::dynamic_pointer_cast<const core::AggregationNode>(node)) {
      if (!agg->isPartial()) {
        return 1;
      }
    }
    if (std::dynamic_pointer_cast<const core::TableWriteNode>(node)) {
      return 1;
    }
  }
  return count;
}

} // namespace detail

void LocalPlanner::plan(const core::PlanNodePtr &plan,
                        const std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>> *bridges,
                        std::vector<std::unique_ptr<DriverFactory>> *driverFactories, size_t maxDrivers) {
  VELOX_CHECK(driverFactories != nullptr, "Driver factories must be provided");
  detail::plan(plan, nullptr, nullptr, OperatorSupplier(), bridges, driverFactories);
  for (auto &factory : *driverFactories) {
    factory->numDrivers = detail::maxDrivers(*factory, maxDrivers);
  }
}

} // namespace facebook::velox::exec
