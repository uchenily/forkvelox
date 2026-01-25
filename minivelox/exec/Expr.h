#pragma once

#include "vector/BaseVector.h"
#include "vector/RowVector.h"
#include "vector/SelectivityVector.h"
#include "type/Variant.h"
#include "core/Context.h"
#include <vector>
#include <sstream>

namespace facebook::velox::exec {

class ExprSet;

struct EvalCtx {
    core::ExecCtx* execCtx;
    ExprSet* exprSet;
    RowVectorPtr input;
    
    EvalCtx(core::ExecCtx* ctx, ExprSet* set, RowVectorPtr in) 
        : execCtx(ctx), exprSet(set), input(in) {}
        
    memory::MemoryPool* pool() const;
};

class Expr {
public:
    virtual ~Expr() = default;
    
    virtual void eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) = 0;
    virtual TypePtr type() const = 0;
    virtual std::string toString() const = 0;
};

using ExprPtr = std::shared_ptr<Expr>;

class FieldReference : public Expr {
public:
    FieldReference(TypePtr type, std::string name, int32_t index) 
        : type_(type), name_(std::move(name)), index_(index) {}

    void eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) override;
    TypePtr type() const override { return type_; }
    std::string toString() const override { return name_; }

private:
    TypePtr type_;
    std::string name_;
    int32_t index_;
};

class ConstantExpr : public Expr {
public:
    ConstantExpr(Variant value) : value_(std::move(value)) {}
    
    void eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) override;
    TypePtr type() const override { return value_.type(); }
    std::string toString() const override { return value_.toString(); }
    
private:
    Variant value_;
};

class ExprSet {
public:
    ExprSet(std::vector<ExprPtr> exprs, core::ExecCtx* execCtx) 
        : exprs_(std::move(exprs)), execCtx_(execCtx) {}

    void eval(const SelectivityVector& rows, EvalCtx& context, std::vector<VectorPtr>& result);
    std::string toString(bool compact = false) const;
    const std::vector<ExprPtr>& exprs() const { return exprs_; }
    core::ExecCtx* execCtx() const { return execCtx_; }

private:
    std::vector<ExprPtr> exprs_;
    core::ExecCtx* execCtx_;
};

} 

namespace facebook::velox::core {
    using TypedExprPtr = std::shared_ptr<exec::Expr>; 
}
