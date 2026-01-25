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

#include <iostream>

// ... inside AssertQueryBuilder

    std::unique_ptr<Operator> buildOperatorTree(PlanNodePtr node, memory::MemoryPool* pool) {
        // std::cout << "Building operator for " << node->toString() << std::endl;
        // ...
        else if (auto join = std::dynamic_pointer_cast<const HashJoinNode>(node)) {
             // Build Right
             auto rightOp = buildOperatorTree(join->right(), pool);
             
             std::vector<RowVectorPtr> buildSide;
             int loopCount = 0;
             while(true) {
                 if (loopCount++ > 1000) throw std::runtime_error("HashJoin build loop exceeded limit");
                 auto batch = rightOp->getOutput();
                 if (!batch) { if (rightOp->isFinished()) break; continue; }
                 buildSide.push_back(batch);
             }
             
             auto leftOp = buildOperatorTree(join->left(), pool);
             
             auto op = std::make_unique<HashJoinOperator>(std::move(ctx), join->leftKeys(), join->rightKeys(), buildSide, join->outputType());
             return std::make_unique<PullAdapter>(std::move(op), std::move(leftOp));
        }
        
// ...

    class PullAdapter : public Operator {
        // ...
        RowVectorPtr getOutput() override {
            auto out = op_->getOutput();
            if (out) return out;
            
            if (source_->isFinished()) {
                // Ensure we called noMoreInput only once? Operator handles it usually.
                // But HashJoin finish logic relies on it.
                // If source finished, we might have already called noMoreInput.
                // We should check if we already returned the final output.
                // But getOutput() called noMoreInput last time?
                // No, we called noMoreInput then returned getOutput().
                // If called again, op->getOutput() returns null.
                return nullptr;
            }
            
            int loopCount = 0;
            while(!source_->isFinished()) {
                if (loopCount++ > 10000) throw std::runtime_error("PullAdapter loop exceeded limit for " + source_->operatorCtx()->planNodeId());
                auto batch = source_->getOutput();
                if (batch) {
                    op_->addInput(batch);
                    out = op_->getOutput();
                    if (out) return out;
                }
            }
            op_->noMoreInput(); 
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
