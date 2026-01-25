#pragma once
#include "velox/vector/ComplexVector.h"
#include "velox/core/PlanNode.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

class Operator {
public:
    Operator(core::PlanNodePtr planNode) : planNode_(planNode) {}
    virtual ~Operator() = default;
    
    virtual void addInput(RowVectorPtr input) = 0;
    virtual RowVectorPtr getOutput() = 0;
    virtual bool isFinished() = 0;
    virtual bool needsInput() const { return true; }
    
    core::PlanNodePtr planNode() const { return planNode_; }

protected:
    core::PlanNodePtr planNode_;
};

class ValuesOperator : public Operator {
public:
    ValuesOperator(core::PlanNodePtr node) : Operator(node) {
        auto valuesNode = std::dynamic_pointer_cast<const core::ValuesNode>(node);
        values_ = valuesNode->values();
    }
    
    void addInput(RowVectorPtr input) override {
        // Source operator, no input
    }
    
    RowVectorPtr getOutput() override {
        if (current_ < values_.size()) {
            return values_[current_++];
        }
        return nullptr;
    }
    
    bool isFinished() override { return current_ >= values_.size(); }
    bool needsInput() const override { return false; }
    
private:
    std::vector<RowVectorPtr> values_;
    size_t current_ = 0;
};

class FilterOperator : public Operator {
public:
    FilterOperator(core::PlanNodePtr node, core::ExecCtx* ctx) : Operator(node), ctx_(ctx) {
        auto filterNode = std::dynamic_pointer_cast<const core::FilterNode>(node);
        std::vector<core::TypedExprPtr> exprs = {filterNode->filter()};
        exprSet_ = std::make_unique<ExprSet>(exprs, ctx);
    }
    
    void addInput(RowVectorPtr input) override {
        input_ = input;
        // Eval filter
        if (input_) {
            EvalCtx evalCtx(ctx_, exprSet_.get(), input_.get());
            SelectivityVector rows(input_->size());
            std::vector<VectorPtr> results;
            exprSet_->eval(rows, evalCtx, results);
            
            // Apply filter to input
            // results[0] should be boolean.
            // Simplified: we just pass through for now or return empty if false.
            // For demo: "a % 2 == 0"
            // We need to actually filter.
            // Assume result is boolean flat vector.
            // ...
        }
    }
    
    RowVectorPtr getOutput() override {
        auto result = input_;
        input_ = nullptr;
        return result; // Pass through for stub
    }
    
    bool isFinished() override { return finished_; }
    
    void noMoreInput() { finished_ = true; }
    
private:
    RowVectorPtr input_;
    core::ExecCtx* ctx_;
    std::unique_ptr<ExprSet> exprSet_;
    bool finished_ = false;
};

// ... Stubs for others
}
