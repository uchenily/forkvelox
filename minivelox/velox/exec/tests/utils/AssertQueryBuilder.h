#pragma once
#include "velox/core/PlanNode.h"
#include "velox/exec/ExecutionDriver.h"
#include "velox/exec/Operator.h"
#include "velox/core/ExecCtx.h"
#include "velox/exec/Split.h"

namespace facebook::velox::exec::test {

class AssertQueryBuilder {
public:
    AssertQueryBuilder(core::PlanNodePtr planNode) : planNode_(planNode) {}
    
    std::shared_ptr<RowVector> copyResults(memory::MemoryPool* pool) {
        auto queryCtx = core::QueryCtx::create();
        core::ExecCtx execCtx(pool, queryCtx.get());
        
        std::vector<std::shared_ptr<Operator>> ops;
        buildPipeline(planNode_, ops, &execCtx);
        std::reverse(ops.begin(), ops.end());
        
        ::facebook::velox::exec::ExecutionDriver driver(ops);
        auto batches = driver.run();
        
        if (batches.empty()) {
             return nullptr; 
        }
        
        return std::dynamic_pointer_cast<RowVector>(batches[0]);
    }
    
    AssertQueryBuilder& split(const core::PlanNodeId& id, exec::Split split) {
        return *this;
    }
    
    AssertQueryBuilder& split(exec::Split split) {
        return *this;
    }

private:
    void buildPipeline(core::PlanNodePtr node, std::vector<std::shared_ptr<Operator>>& ops, core::ExecCtx* ctx) {
        if (auto values = std::dynamic_pointer_cast<const core::ValuesNode>(node)) {
            ops.push_back(std::make_shared<ValuesOperator>(node));
        } else if (auto filter = std::dynamic_pointer_cast<const core::FilterNode>(node)) {
            ops.push_back(std::make_shared<FilterOperator>(node, ctx));
        } else if (auto agg = std::dynamic_pointer_cast<const core::AggregationNode>(node)) {
            ops.push_back(std::make_shared<AggregationOperator>(node, ctx));
        } else if (auto orderBy = std::dynamic_pointer_cast<const core::OrderByNode>(node)) {
            ops.push_back(std::make_shared<OrderByOperator>(node));
        } else if (auto topN = std::dynamic_pointer_cast<const core::TopNNode>(node)) {
            ops.push_back(std::make_shared<TopNOperator>(node));
        } else if (auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
            ops.push_back(std::make_shared<HashJoinOperator>(node));
        } else if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(node)) {
            // Scan returns nothing for now as we don't have data source
            ops.push_back(std::make_shared<ValuesOperator>(
                std::make_shared<core::ValuesNode>(scan->id(), std::vector<RowVectorPtr>{}))); 
        } else {
             // Pass through for others
             ops.push_back(std::make_shared<PassThroughOperator>(node));
        }
        
        auto sources = node->sources();
        if (!sources.empty()) {
            buildPipeline(sources[0], ops, ctx);
        }
    }

    core::PlanNodePtr planNode_;
};

}
