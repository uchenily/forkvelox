#pragma once

#include "exec/Operator.h"
#include "exec/PlanNode.h"
#include "exec/Expr.h"
#include "vector/SimpleVector.h"
#include "vector/RowVector.h"
#include <deque>

namespace facebook::velox::exec {

// ValuesOperator
class ValuesOperator : public Operator {
public:
    ValuesOperator(std::unique_ptr<OperatorCtx> ctx, const std::shared_ptr<const core::ValuesNode>& node)
        : Operator(std::move(ctx)), values_(node->values()), current_(0) {}

    void addInput(RowVectorPtr input) override {
        // Source
    }

    RowVectorPtr getOutput() override {
        if (current_ < values_.size()) {
            return values_[current_++];
        }
        return nullptr;
    }

    bool isFinished() override {
        return current_ >= values_.size();
    }

private:
    std::vector<RowVectorPtr> values_;
    size_t current_;
};

// FilterOperator
class FilterOperator : public Operator {
public:
    FilterOperator(std::unique_ptr<OperatorCtx> ctx, const std::shared_ptr<const core::FilterNode>& node)
        : Operator(std::move(ctx)), filter_(node->filter()) {}

    void addInput(RowVectorPtr input) override {
        input_ = input;
    }

    RowVectorPtr getOutput() override {
        if (!input_) return nullptr;
        
        core::ExecCtx* execCtx = ctx_->execCtx(); 
        
        EvalCtx evalCtx(execCtx, nullptr, input_);
        
        SelectivityVector rows(input_->size());
        VectorPtr result;
        filter_->eval(rows, evalCtx, result);
        
        auto boolVec = std::dynamic_pointer_cast<FlatVector<uint8_t>>(result);
        
        std::vector<vector_size_t> indices;
        for (vector_size_t i = 0; i < input_->size(); ++i) {
            if (boolVec->valueAt(i)) {
                indices.push_back(i);
            }
        }
        
        if (indices.empty()) {
            // Return empty row vector of correct type
             auto rowType = asRowType(input_->type());
             std::vector<VectorPtr> emptyChildren;
             for(size_t i=0; i<rowType->size(); ++i) {
                  // Keep children structure but length 0. 
                  // BaseVector doesn't imply ownership of data buffer in a way that prevents sharing if length is 0.
                  // But we need valid VectorPtrs.
                  // Let's use the input children but creating a RowVector with length 0 is valid.
                  emptyChildren.push_back(input_->childAt(i));
             }
             auto ret = std::make_shared<RowVector>(ctx_->pool(), input_->type(), nullptr, 0, emptyChildren);
             input_ = nullptr;
             return ret;
        }
        
        if (indices.size() == input_->size()) {
             auto ret = input_;
             input_ = nullptr;
             return ret;
        }
        
        // Copy
        std::vector<VectorPtr> newChildren;
        auto rowType = asRowType(input_->type());
        for (size_t c = 0; c < input_->children().size(); ++c) {
             auto child = input_->childAt(c);
             newChildren.push_back(copyVector(child, indices, ctx_->pool()));
        }
        
        auto ret = std::make_shared<RowVector>(ctx_->pool(), rowType, nullptr, indices.size(), newChildren);
        input_ = nullptr;
        return ret;
    }

    bool isFinished() override { return false; }

private:
    exec::ExprPtr filter_;
    RowVectorPtr input_;

    // Helpers to copy
    template<typename T>
    VectorPtr copySimple(std::shared_ptr<FlatVector<T>> vec, const std::vector<vector_size_t>& indices, memory::MemoryPool* pool) {
        std::vector<T> newData;
        newData.reserve(indices.size());
        for (auto idx : indices) newData.push_back(vec->valueAt(idx));
        return makeFlatVector(newData, pool);
    }

    VectorPtr copyVector(VectorPtr vec, const std::vector<vector_size_t>& indices, memory::MemoryPool* pool) {
        if (auto v = std::dynamic_pointer_cast<FlatVector<int32_t>>(vec)) return copySimple(v, indices, pool);
        if (auto v = std::dynamic_pointer_cast<FlatVector<int64_t>>(vec)) return copySimple(v, indices, pool);
        if (auto v = std::dynamic_pointer_cast<FlatVector<StringView>>(vec)) return copySimple(v, indices, pool);
        if (auto v = std::dynamic_pointer_cast<FlatVector<uint8_t>>(vec)) return copySimple(v, indices, pool); 
        return nullptr;
    }
};

// ProjectOperator
class ProjectOperator : public Operator {
public:
    ProjectOperator(std::unique_ptr<OperatorCtx> ctx, const std::shared_ptr<const core::ProjectNode>& node)
        : Operator(std::move(ctx)), projections_(node->projections()), names_(node->names()) {}

    void addInput(RowVectorPtr input) override {
        input_ = input;
    }

    RowVectorPtr getOutput() override {
        if (!input_) return nullptr;
        
        core::ExecCtx* execCtx = ctx_->execCtx();
        EvalCtx evalCtx(execCtx, nullptr, input_);
        SelectivityVector rows(input_->size());
        
        std::vector<VectorPtr> outputs;
        for (auto& expr : projections_) {
            VectorPtr result;
            expr->eval(rows, evalCtx, result);
            outputs.push_back(result);
        }
        
        auto ret = makeRowVector(names_, outputs, ctx_->pool());
        input_ = nullptr;
        return ret;
    }
    
    bool isFinished() override { return false; }

private:
    std::vector<exec::ExprPtr> projections_;
    std::vector<std::string> names_;
    RowVectorPtr input_;
};

} // namespace facebook::velox::exec