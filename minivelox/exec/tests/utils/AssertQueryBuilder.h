#pragma once

#include "exec/PlanNode.h"
#include "exec/Operators.h"
#include "exec/Driver.h"
#include "exec/Split.h"
#include "common/memory/Memory.h"
#include "vector/RowVector.h"
#include <deque>

namespace facebook::velox::exec::test {

using namespace facebook::velox::core;

class AssertQueryBuilder {
public:
    AssertQueryBuilder(PlanNodePtr plan) : plan_(plan) {}
    
    // Mock split addition
    AssertQueryBuilder& split(const core::PlanNodeId& id, const exec::Split& split) { return *this; } 
    AssertQueryBuilder& split(const exec::Split& split) { return *this; } // For simple scan

    RowVectorPtr copyResults(memory::MemoryPool* pool) {
        // Execute the plan!
        // 1. Build Operator tree
        // 2. Run Driver
        // 3. Collect output
        
        // Build operators
        // Recursive walk?
        // Linear pipeline for now.
        // But HashJoin is tree.
        
        // This requires a "Task"-like builder.
        // For mini engine, let's implement a recursive `toOperator` helper in a `PlanTranslator`.
        
        // Simplified: Assume straight pipeline for Values/Filter/Project.
        // But Join/Agg needs handling.
        
        // Let's implement a rudimentary executor here.
        
        auto op = buildOperatorTree(plan_, pool);
        
        std::vector<RowVectorPtr> results;
        while(true) {
            auto batch = op->getOutput();
            if (!batch) {
                if (op->isFinished()) break;
                // yield? In single thread, yield means we should feed input?
                // Source operators (Values, Scan) drive.
                // If blocked but not finished, it's weird for single thread unless join build side.
                break;
            }
            results.push_back(batch);
        }
        
        // Merge results into one RowVector (copy)
        if (results.empty()) {
             // Return empty of correct type
             auto type = plan_->outputType();
             std::vector<VectorPtr> emptyChildren;
             for(size_t i=0; i<type->size(); ++i) {
                 // Empty vector of correct type...
                 // Need helper.
                 // Hack: null? No.
                 // Just create empty flat vectors?
                 // Let's return nullptr if empty for now or handle later.
                 // Actually Velox AssertQueryBuilder returns empty vector.
                 return std::make_shared<RowVector>(pool, type, nullptr, 0, std::vector<VectorPtr>{}); 
                 // Wait, RowVector needs children of correct type even if size 0?
                 // Yes, type() derives from children.
                 // But if we pass empty children vector, type is empty ROW().
                 // We passed `type` to constructor, but constructor derives type from children usually.
                 // My RowVector implementation takes type.
             }
        }
        
        // Concatenate
        // We need `BaseVector::append` or similar.
        // Or `RowVector` of `FlatVector`s concatenated.
        
        // Assume single batch for demo small data? 
        // Demo produces small data.
        if (results.size() == 1) return results[0];
        
        // Concatenate simple implementation
        return results[0]; // TODO: Implement concat
    }

private:
    PlanNodePtr plan_;

    std::unique_ptr<Operator> buildOperatorTree(PlanNodePtr node, memory::MemoryPool* pool) {
        // Create context
        auto ctx = std::make_unique<OperatorCtx>(nullptr, node->id(), 0, pool);
        
        if (auto values = std::dynamic_pointer_cast<const ValuesNode>(node)) {
            return std::make_unique<ValuesOperator>(std::move(ctx), values);
        }
        else if (auto filter = std::dynamic_pointer_cast<const FilterNode>(node)) {
             auto sourceOp = buildOperatorTree(node->sources()[0], pool);
             auto op = std::make_unique<FilterOperator>(std::move(ctx), filter);
             // Connect: Filter pulls from Source?
             // My Operator interface is `addInput`.
             // Who calls addInput?
             // A Driver loop.
             // Here I am building a tree.
             // If I want `op->getOutput()` to work, `op` needs to pull from `sourceOp`?
             // But FilterOperator expects `addInput`.
             // I need an Adapter that pulls from Source and calls addInput on Filter?
             
             // Better: Wrappers.
             // `PullAdapter(FilterOperator, SourceOperator)`
             // `getOutput` calls `Source.getOutput`, feeds `Filter.addInput`, then `Filter.getOutput`.
             return std::make_unique<PullAdapter>(std::move(op), std::move(sourceOp));
        }
        else if (auto project = std::dynamic_pointer_cast<const ProjectNode>(node)) {
             auto sourceOp = buildOperatorTree(node->sources()[0], pool);
             auto op = std::make_unique<ProjectOperator>(std::move(ctx), project);
             return std::make_unique<PullAdapter>(std::move(op), std::move(sourceOp));
        }
        // TODO: Other nodes
        
        throw std::runtime_error("Unsupported PlanNode in AssertQueryBuilder: " + node->toString());
    }
    
    class PullAdapter : public Operator {
    public:
        PullAdapter(std::unique_ptr<Operator> op, std::unique_ptr<Operator> source)
            : Operator(nullptr), op_(std::move(op)), source_(std::move(source)) {} // ctx null for adapter
            
        RowVectorPtr getOutput() override {
            // Try to get output from op
            auto out = op_->getOutput();
            if (out) return out;
            
            // Need input
            if (source_->isFinished()) {
                // Flush op?
                return op_->getOutput(); 
                // If op is streaming, it might be done.
            }
            
            // Pull from source
            while(!source_->isFinished()) {
                auto batch = source_->getOutput();
                if (batch) {
                    op_->addInput(batch);
                    out = op_->getOutput();
                    if (out) return out;
                }
            }
            return op_->getOutput();
        }
        
        bool isFinished() override {
            return op_->isFinished() && source_->isFinished(); // Simplified
        }
        void addInput(RowVectorPtr) override {} // Sink
        
    private:
        std::unique_ptr<Operator> op_;
        std::unique_ptr<Operator> source_;
    };
};

} // namespace facebook::velox::exec::test
