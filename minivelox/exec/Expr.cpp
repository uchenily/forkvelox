#include "exec/Expr.h"
#include "exec/Function.h"
#include "vector/SimpleVector.h"
#include "type/StringView.h"
#include <iostream>
#include <cmath>
#include <algorithm>
#include <cctype>

namespace facebook::velox::exec {

memory::MemoryPool* EvalCtx::pool() const { return execCtx->pool(); }

// ExprSet
void ExprSet::eval(const SelectivityVector& rows, EvalCtx& context, std::vector<VectorPtr>& result) {
    result.resize(exprs_.size());
    for (size_t i = 0; i < exprs_.size(); ++i) {
        exprs_[i]->eval(rows, context, result[i]);
    }
}

std::string ExprSet::toString(bool compact) const {
    std::stringstream ss;
    for (const auto& e : exprs_) ss << e->toString() << "\n";
    return ss.str();
}

// FieldReference
void FieldReference::eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) {
    // Sharing child vector (zero-copy)
    result = context.input->childAt(index_);
}

// ConstantExpr
void ConstantExpr::eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) {
    TypePtr t = value_.type();
    auto pool = context.pool();
    vector_size_t size = context.input->size(); 
    
    if (t->kind() == TypeKind::INTEGER) {
        int32_t val = value_.value<int32_t>();
        std::vector<int32_t> data(size, val);
        result = makeFlatVector(data, pool);
    } else if (t->kind() == TypeKind::BIGINT) {
        int64_t val = value_.value<int64_t>();
        std::vector<int64_t> data(size, val);
        result = makeFlatVector(data, pool);
    } else if (t->kind() == TypeKind::VARCHAR) {
        std::string valStr = value_.value<StringView>().str();
        std::vector<std::string> data(size, valStr);
        result = makeFlatVector(data, pool);
    } else {
        throw std::runtime_error("Constant type not supported");
    }
}

// FunctionExpr
void FunctionExpr::eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) {
    std::vector<VectorPtr> args(inputs_.size());
    for (size_t i = 0; i < inputs_.size(); ++i) {
        inputs_[i]->eval(rows, context, args[i]);
    }
    function_->apply(rows, args, outputType_, context, result);
}

std::string FunctionExpr::toString() const {
    std::stringstream ss;
    ss << name_ << "(";
    for (size_t i = 0; i < inputs_.size(); ++i) {
        if (i > 0) ss << ", ";
        ss << inputs_[i]->toString();
    }
    ss << ")";
    return ss.str();
}

// Registry Impl
SimpleFunctionRegistry& SimpleFunctionRegistry::instance() {
    static SimpleFunctionRegistry instance;
    return instance;
}

void SimpleFunctionRegistry::registerFunction(const std::string& name, std::shared_ptr<VectorFunction> func) {
    functions_[name] = func;
}

std::shared_ptr<VectorFunction> SimpleFunctionRegistry::getFunction(const std::string& name) {
    auto it = functions_.find(name);
    if (it != functions_.end()) return it->second;
    return nullptr;
}

// Implementations of specific functions
// Helpers
template<typename T>
void ensureSize(VectorPtr& result, vector_size_t size, memory::MemoryPool* pool) {
    if (!result || result->size() != size) {
        std::vector<T> dummy(size);
        result = makeFlatVector(dummy, pool);
    }
}

// Arithmetic: Plus
class PlusFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, const std::vector<VectorPtr>& args, const TypePtr& outputType,
               EvalCtx& context, VectorPtr& result) const override {
        // Assume binary int64 for demo
        auto lhs = std::dynamic_pointer_cast<FlatVector<int64_t>>(args[0]);
        auto rhs = std::dynamic_pointer_cast<FlatVector<int64_t>>(args[1]);
        
        ensureSize<int64_t>(result, rows.size(), context.pool());
        auto resVec = std::dynamic_pointer_cast<FlatVector<int64_t>>(result);
        auto rawRes = resVec->mutableRawValues();
        
        rows.applyToSelected([&](vector_size_t i) {
             rawRes[i] = lhs->valueAt(i) + rhs->valueAt(i);
        });
    }
};

// Arithmetic: Multiply
class MultiplyFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, const std::vector<VectorPtr>& args, const TypePtr& outputType,
               EvalCtx& context, VectorPtr& result) const override {
        auto lhs = std::dynamic_pointer_cast<FlatVector<int64_t>>(args[0]);
        auto rhs = std::dynamic_pointer_cast<FlatVector<int64_t>>(args[1]);
        
        ensureSize<int64_t>(result, rows.size(), context.pool());
        auto resVec = std::dynamic_pointer_cast<FlatVector<int64_t>>(result);
        auto rawRes = resVec->mutableRawValues();
        
        rows.applyToSelected([&](vector_size_t i) {
             rawRes[i] = lhs->valueAt(i) * rhs->valueAt(i);
        });
    }
};

// Arithmetic: Modulo
class ModuloFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, const std::vector<VectorPtr>& args, const TypePtr& outputType,
               EvalCtx& context, VectorPtr& result) const override {
        auto lhs = std::dynamic_pointer_cast<FlatVector<int64_t>>(args[0]);
        auto rhs = std::dynamic_pointer_cast<FlatVector<int64_t>>(args[1]);
        
        ensureSize<int64_t>(result, rows.size(), context.pool());
        auto resVec = std::dynamic_pointer_cast<FlatVector<int64_t>>(result);
        auto rawRes = resVec->mutableRawValues();
        
        rows.applyToSelected([&](vector_size_t i) {
             rawRes[i] = lhs->valueAt(i) % rhs->valueAt(i);
        });
    }
};

// String: Substr
class SubstrFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, const std::vector<VectorPtr>& args, const TypePtr& outputType,
               EvalCtx& context, VectorPtr& result) const override {
        auto input = std::dynamic_pointer_cast<FlatVector<StringView>>(args[0]);
        // args[1] start, args[2] length
        auto startVec = std::dynamic_pointer_cast<FlatVector<int64_t>>(args[1]); 
        auto lenVec = std::dynamic_pointer_cast<FlatVector<int64_t>>(args[2]);
        
        std::vector<std::string> resStrings(rows.size());
        
        rows.applyToSelected([&](vector_size_t i) {
            StringView str = input->valueAt(i);
            int64_t start = startVec->valueAt(i); // 1-based index in SQL usually
            int64_t len = lenVec->valueAt(i);
            
            // Adjust 1-based to 0-based
            int64_t sIdx = start - 1;
            if (sIdx < 0) sIdx = 0;
            if (sIdx >= str.size()) {
                resStrings[i] = "";
            } else {
                int32_t actualLen = std::min((int64_t)str.size() - sIdx, len);
                resStrings[i] = std::string(str.data() + sIdx, actualLen);
            }
        });
        
        result = makeFlatVector(resStrings, context.pool());
    }
};

// String: Upper
class UpperFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, const std::vector<VectorPtr>& args, const TypePtr& outputType,
               EvalCtx& context, VectorPtr& result) const override {
        auto input = std::dynamic_pointer_cast<FlatVector<StringView>>(args[0]);
        std::vector<std::string> resStrings(rows.size());
        
        rows.applyToSelected([&](vector_size_t i) {
            std::string s = input->valueAt(i).str();
            std::transform(s.begin(), s.end(), s.begin(), ::toupper);
            resStrings[i] = s;
        });
        result = makeFlatVector(resStrings, context.pool());
    }
};

// String: Concat
class ConcatFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, const std::vector<VectorPtr>& args, const TypePtr& outputType,
               EvalCtx& context, VectorPtr& result) const override {
        auto lhs = std::dynamic_pointer_cast<FlatVector<StringView>>(args[0]);
        auto rhs = std::dynamic_pointer_cast<FlatVector<StringView>>(args[1]);
        
        std::vector<std::string> resStrings(rows.size());
        rows.applyToSelected([&](vector_size_t i) {
             resStrings[i] = lhs->valueAt(i).str() + rhs->valueAt(i).str();
        });
        result = makeFlatVector(resStrings, context.pool());
    }
};

// Filter: Eq 
class EqFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, const std::vector<VectorPtr>& args, const TypePtr& outputType,
               EvalCtx& context, VectorPtr& result) const override {
         // Assume int64 for demo
        auto lhs = std::dynamic_pointer_cast<FlatVector<int64_t>>(args[0]);
        auto rhs = std::dynamic_pointer_cast<FlatVector<int64_t>>(args[1]);
        
        std::vector<uint8_t> resData(rows.size());
        rows.applyToSelected([&](vector_size_t i) {
            resData[i] = (lhs->valueAt(i) == rhs->valueAt(i)) ? 1 : 0;
        });
        
        auto pool = context.pool();
        auto dataBuf = AlignedBuffer::allocate(rows.size(), pool);
        std::memcpy(dataBuf->asMutable<uint8_t>(), resData.data(), rows.size());
        
        // Return as IntegerType but conceptually Boolean (velox stores boolean as bits, simplified to bytes here in memory, but exposed as vector)
        // I'll make a helper for boolean vector later if needed.
        // Reusing IntegerType as placeholder for Boolean in Type system for now.
        result = std::make_shared<FlatVector<uint8_t>>(pool, std::make_shared<IntegerType>(), nullptr, rows.size(), dataBuf); 
    }
};

// Dummy Aggregate Functions (Scalar placeholders)
class AggregatePlaceholderFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, const std::vector<VectorPtr>& args, const TypePtr& outputType,
               EvalCtx& context, VectorPtr& result) const override {
        throw std::runtime_error("Aggregate function called in scalar context");
    }
};

// Register all functions
void registerAllFunctions() {
    auto& reg = SimpleFunctionRegistry::instance();
    reg.registerFunction("plus", std::make_shared<PlusFunction>());
    reg.registerFunction("multiply", std::make_shared<MultiplyFunction>());
    reg.registerFunction("mod", std::make_shared<ModuloFunction>());
    reg.registerFunction("substr", std::make_shared<SubstrFunction>());
    reg.registerFunction("upper", std::make_shared<UpperFunction>());
    reg.registerFunction("concat", std::make_shared<ConcatFunction>());
    reg.registerFunction("eq", std::make_shared<EqFunction>());
    
    // Aggregates
    auto placeholder = std::make_shared<AggregatePlaceholderFunction>();
    reg.registerFunction("sum", placeholder);
    reg.registerFunction("avg", placeholder);
    reg.registerFunction("count", placeholder);
}

} // namespace facebook::velox::exec