#pragma once

#include "vector/BaseVector.h"
#include "vector/RowVector.h"
#include "vector/SelectivityVector.h"
#include <vector>
#include <sstream>

namespace facebook::velox::core {
    class QueryCtx;
    class ExecCtx;
}

namespace facebook::velox::exec {

class ExprSet;

struct EvalCtx {
    core::ExecCtx* execCtx;
    ExprSet* exprSet;
    RowVectorPtr input;
    
    EvalCtx(core::ExecCtx* ctx, ExprSet* set, RowVectorPtr in) 
        : execCtx(ctx), exprSet(set), input(in) {}
};

class Expr {
public:
    virtual ~Expr() = default;
    
    // Evaluate expression on selected rows and store result in 'result'
    // If result is null, allocate it.
    virtual void eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) = 0;
    
    virtual TypePtr type() const = 0;
    
    virtual std::string toString() const = 0;
};

using ExprPtr = std::shared_ptr<Expr>;

class ExprSet {
public:
    ExprSet(std::vector<ExprPtr> exprs, core::ExecCtx* execCtx) 
        : exprs_(std::move(exprs)), execCtx_(execCtx) {}

    void eval(const SelectivityVector& rows, EvalCtx& context, std::vector<VectorPtr>& result) {
        result.resize(exprs_.size());
        for (size_t i = 0; i < exprs_.size(); ++i) {
            exprs_[i]->eval(rows, context, result[i]);
        }
    }
    
    std::string toString(bool compact = false) const {
        std::stringstream ss;
        for (const auto& e : exprs_) ss << e->toString() << "\n";
        return ss.str();
    }
    
    const std::vector<ExprPtr>& exprs() const { return exprs_; }
    core::ExecCtx* execCtx() const { return execCtx_; }

private:
    std::vector<ExprPtr> exprs_;
    core::ExecCtx* execCtx_;
};

} 

namespace facebook::velox::core {
    class QueryCtx {
    public:
         static std::shared_ptr<QueryCtx> create(std::shared_ptr<void> executor = nullptr) {
             return std::make_shared<QueryCtx>();
         }
    };
    
    class ExecCtx {
    public:
        ExecCtx(memory::MemoryPool* pool, QueryCtx* queryCtx) : pool_(pool), queryCtx_(queryCtx) {}
        memory::MemoryPool* pool() const { return pool_; }
    private:
        memory::MemoryPool* pool_;
        QueryCtx* queryCtx_;
    };
    
    using TypedExprPtr = std::shared_ptr<exec::Expr>; 
}