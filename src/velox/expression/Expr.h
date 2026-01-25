#pragma once

#include <memory>
#include <vector>
#include <string>
#include "velox/core/ITypedExpr.h"
#include "velox/expression/EvalCtx.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {

class Expr {
public:
    Expr(std::shared_ptr<const Type> type, std::vector<std::shared_ptr<Expr>> inputs, std::string name)
        : type_(std::move(type)), inputs_(std::move(inputs)), name_(std::move(name)) {}

    virtual ~Expr() = default;
    
    virtual void eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) = 0;
    
    std::shared_ptr<const Type> type() const { return type_; }
    const std::vector<std::shared_ptr<Expr>>& inputs() const { return inputs_; }
    const std::string& name() const { return name_; }

    virtual std::string toString(bool recursive = true) const;
    
    // Stub for stats
    struct Stats {
        uint64_t numProcessedRows = 0;
        uint64_t numProcessedVectors = 0;
        struct { uint64_t cpuNanos = 0; } timing;
    };
    Stats stats() const { return {}; }

protected:
    std::shared_ptr<const Type> type_;
    std::vector<std::shared_ptr<Expr>> inputs_;
    std::string name_;
    
    void appendInputs(std::stringstream& stream) const;
};

using ExprPtr = std::shared_ptr<Expr>;

class ExprSet {
public:
    ExprSet(std::vector<std::shared_ptr<core::ITypedExpr>> sources, core::ExecCtx* execCtx);
    ~ExprSet();

    void eval(const SelectivityVector& rows, EvalCtx& context, std::vector<VectorPtr>& result);
    
    std::string toString(bool compact = true) const;
    
    const std::vector<ExprPtr>& exprs() const { return exprs_; }

private:
    std::vector<std::shared_ptr<core::ITypedExpr>> sources_;
    core::ExecCtx* execCtx_;
    std::vector<ExprPtr> exprs_; // Compiled exprs
};

}