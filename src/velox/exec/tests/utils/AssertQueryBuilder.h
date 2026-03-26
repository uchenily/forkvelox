#pragma once
#include "velox/core/ExecCtx.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Split.h"
#include "velox/tpch/gen/TpchGen.h"
#include <unordered_map>

namespace facebook::velox::exec::test {

class AssertQueryBuilder {
public:
  AssertQueryBuilder(core::PlanNodePtr planNode) : planNode_(planNode) {}

  std::shared_ptr<RowVector> copyResults(memory::MemoryPool *pool) {
    auto queryCtx = core::QueryCtx::create();
    core::ExecCtx execCtx(pool, queryCtx.get());
    std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>> bridges;
    buildAllJoins(planNode_, &execCtx, bridges);

    std::vector<std::shared_ptr<Operator>> ops;
    buildPipeline(planNode_, ops, &execCtx, bridges);
    std::reverse(ops.begin(), ops.end());

    ::facebook::velox::exec::Driver driver(ops);
    std::vector<RowVectorPtr> batches;
    driver.run(batches);

    if (batches.empty()) {
      return nullptr;
    }

    return std::dynamic_pointer_cast<RowVector>(batches[0]);
  }

  AssertQueryBuilder &split(const core::PlanNodeId &id, exec::Split split) { return *this; }

  AssertQueryBuilder &split(exec::Split split) { return *this; }

private:
  void buildPipeline(core::PlanNodePtr node, std::vector<std::shared_ptr<Operator>> &ops, core::ExecCtx *ctx,
                     const std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>> &bridges) {
    if (auto values = std::dynamic_pointer_cast<const core::ValuesNode>(node)) {
      ops.push_back(std::make_shared<ValuesOperator>(node));
    } else if (std::dynamic_pointer_cast<const core::FileScanNode>(node)) {
      ops.push_back(std::make_shared<FileScanOperator>(node, ctx));
    } else if (std::dynamic_pointer_cast<const core::TableWriteNode>(node)) {
      ops.push_back(std::make_shared<TableWriteOperator>(node));
    } else if (auto filter = std::dynamic_pointer_cast<const core::FilterNode>(node)) {
      ops.push_back(std::make_shared<FilterOperator>(node, ctx));
    } else if (auto agg = std::dynamic_pointer_cast<const core::AggregationNode>(node)) {
      ops.push_back(std::make_shared<AggregationOperator>(node, ctx));
    } else if (auto orderBy = std::dynamic_pointer_cast<const core::OrderByNode>(node)) {
      ops.push_back(std::make_shared<OrderByOperator>(node));
    } else if (auto topN = std::dynamic_pointer_cast<const core::TopNNode>(node)) {
      ops.push_back(std::make_shared<TopNOperator>(node));
    } else if (auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
      auto it = bridges.find(join->id());
      if (it == bridges.end()) {
        VELOX_FAIL("Missing HashJoin bridge for plan node");
      }
      ops.push_back(std::make_shared<HashProbeOperator>(node, ctx, it->second));
    } else if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(node)) {
      auto projectedData = tpch::readTable(ctx->pool(), scan->table(), scan->columns(), scan->scaleFactor());
      ops.push_back(std::make_shared<ValuesOperator>(
          std::make_shared<core::ValuesNode>(scan->id(), std::vector<RowVectorPtr>{projectedData})));
    } else {
      ops.push_back(std::make_shared<PassThroughOperator>(node));
    }

    auto sources = node->sources();
    if (!sources.empty()) {
      buildPipeline(sources[0], ops, ctx, bridges);
    }
  }

  void buildAllJoins(const core::PlanNodePtr &node, core::ExecCtx *ctx,
                     std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>> &bridges) {
    if (!node) {
      return;
    }
    for (const auto &source : node->sources()) {
      buildAllJoins(source, ctx, bridges);
    }
    auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(node);
    if (!join) {
      return;
    }
    if (bridges.find(join->id()) != bridges.end()) {
      return;
    }
    auto bridge = std::make_shared<HashJoinBridge>();
    bridges.emplace(join->id(), bridge);
    std::vector<std::shared_ptr<Operator>> buildOps;
    buildPipeline(join->sources()[1], buildOps, ctx, bridges);
    std::reverse(buildOps.begin(), buildOps.end());
    buildOps.push_back(std::make_shared<HashBuildOperator>(node, bridge));
    ::facebook::velox::exec::Driver buildDriver(buildOps);
    std::vector<RowVectorPtr> buildResults;
    buildDriver.run(buildResults);
  }

  core::PlanNodePtr planNode_;
};

} // namespace facebook::velox::exec::test
