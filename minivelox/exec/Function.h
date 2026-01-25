#pragma once

#include "exec/Expr.h"
#include "vector/SimpleVector.h"
#include <functional>
#include <unordered_map>

namespace facebook::velox::exec {

class VectorFunction {
public:
    virtual ~VectorFunction() = default;
    virtual void apply(
        const SelectivityVector& rows,
        const std::vector<VectorPtr>& args,
        const TypePtr& outputType,
        EvalCtx& context,
        VectorPtr& result) const = 0;
};

class SimpleFunctionRegistry {
public:
    static SimpleFunctionRegistry& instance();
    
    void registerFunction(const std::string& name, std::shared_ptr<VectorFunction> func);
    std::shared_ptr<VectorFunction> getFunction(const std::string& name);

private:
    std::unordered_map<std::string, std::shared_ptr<VectorFunction>> functions_;
};

class FunctionExpr : public Expr {
public:
    FunctionExpr(std::string name, std::vector<ExprPtr> inputs, TypePtr outputType)
        : name_(std::move(name)), inputs_(std::move(inputs)), outputType_(std::move(outputType)) {
        function_ = SimpleFunctionRegistry::instance().getFunction(name_);
        if (!function_) throw std::runtime_error("Function not found: " + name_);
    }

    void eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) override;
    TypePtr type() const override { return outputType_; }
    std::string toString() const override;

private:
    std::string name_;
    std::vector<ExprPtr> inputs_;
    TypePtr outputType_;
    std::shared_ptr<VectorFunction> function_;
};

// Utilities to help implementations
template<typename T>
T* getRawValues(VectorPtr vec) {
    auto simple = std::dynamic_pointer_cast<SimpleVector<T>>(vec);
    if (!simple) throw std::runtime_error("Expected SimpleVector");
    return simple->mutableRawValues();
}

template<typename T>
const T* getRawValuesConst(VectorPtr vec) {
    auto simple = std::dynamic_pointer_cast<SimpleVector<T>>(vec);
    if (!simple) throw std::runtime_error("Expected SimpleVector");
    return simple->rawValues();
}

} // namespace facebook::velox::exec
