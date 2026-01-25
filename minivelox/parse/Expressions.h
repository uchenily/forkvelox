#pragma once

#include "type/Type.h"
#include "exec/Expr.h" // For TypedExprPtr (ExprPtr)

namespace facebook::velox::parse {
    class IExpr;
}

namespace facebook::velox::core {

class Expressions {
public:
    static std::shared_ptr<exec::Expr> inferTypes(
        const std::shared_ptr<parse::IExpr>& untyped,
        const std::shared_ptr<const RowType>& rowType,
        memory::MemoryPool* pool);
};

} // namespace facebook::velox::core

namespace facebook::velox::parse {

class IExpr {
public:
    virtual ~IExpr() = default;
    virtual std::string toString() const = 0;
};

using IExprPtr = std::shared_ptr<IExpr>;

class DuckSqlExpressionsParser {
public:
    IExprPtr parseExpr(const std::string& text);
};

// Simple AST nodes
class IIdentifier : public IExpr {
public:
    IIdentifier(std::string name) : name_(std::move(name)) {}
    std::string toString() const override { return name_; }
    std::string name_;
};

class ILiteral : public IExpr {
public:
    ILiteral(std::string value) : value_(std::move(value)) {} // Simplified, storing as string
    std::string toString() const override { return value_; }
    std::string value_;
};

class IFunction : public IExpr {
public:
    IFunction(std::string name, std::vector<IExprPtr> args) 
        : name_(std::move(name)), args_(std::move(args)) {}
    std::string toString() const override {
        std::string s = name_ + "(";
        for (size_t i = 0; i < args_.size(); ++i) {
            if (i > 0) s += ", ";
            s += args_[i]->toString();
        }
        s += ")";
        return s;
    }
    std::string name_;
    std::vector<IExprPtr> args_;
};

} // namespace facebook::velox::parse
