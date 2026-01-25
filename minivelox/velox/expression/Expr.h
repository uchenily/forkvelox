#pragma once

#include <memory>
#include <vector>
#include "velox/core/ITypedExpr.h"
#include "velox/expression/EvalCtx.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {

class Expr {
public:
    virtual ~Expr() = default;
    virtual void eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) = 0;
    virtual std::shared_ptr<const Type> type() const = 0;
};

using ExprPtr = std::shared_ptr<Expr>;

class ExprSet {
public:
    ExprSet(std::vector<std::shared_ptr<core::ITypedExpr>> sources, core::ExecCtx* execCtx);
    ~ExprSet();

    void eval(const SelectivityVector& rows, EvalCtx& context, std::vector<VectorPtr>& result);
    
    std::string toString(bool compact = true) const;

private:
    std::vector<std::shared_ptr<core::ITypedExpr>> sources_;
    core::ExecCtx* execCtx_;
    std::vector<ExprPtr> exprs_; // Compiled exprs
};

}
