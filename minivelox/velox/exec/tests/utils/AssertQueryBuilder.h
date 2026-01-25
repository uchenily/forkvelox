#pragma once
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/core/ExecCtx.h"
#include "velox/exec/Split.h"
#include <algorithm>

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
        
        Driver driver(ops);
        auto batches = driver.run();
        
        if (batches.empty()) {
             // Return empty row vector with correct schema?
             // Or just nullptr.
             return nullptr; 
        }
        
        // Should merge batches. For now return first.
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
        } else if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(node)) {
            // Stub scan operator
            ops.push_back(std::make_shared<ValuesOperator>(
                std::make_shared<core::ValuesNode>(scan->id(), std::vector<RowVectorPtr>{}))); 
                // Return empty for scan
        } else {
             // Fallback for Aggregation, etc. - Use ValuesOperator with empty or dummy
             // Or create a PassThroughOperator
             ops.push_back(std::make_shared<ValuesOperator>(
                 std::make_shared<core::ValuesNode>(node->id(), std::vector<RowVectorPtr>{})));
        }
        
        auto sources = node->sources();
        if (!sources.empty()) {
            buildPipeline(sources[0], ops, ctx);
        }
    }

    core::PlanNodePtr planNode_;
};

}
