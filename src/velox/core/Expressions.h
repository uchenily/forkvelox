#pragma once
#include "velox/common/memory/MemoryPool.h"
#include "velox/core/ITypedExpr.h"
#include "velox/type/Variant.h"
#include <vector>
#include <stdexcept>

namespace facebook::velox::core {

class FieldAccessTypedExpr : public ITypedExpr {
public:
    FieldAccessTypedExpr(std::shared_ptr<const Type> type, std::string name)
        : type_(type), name_(name) {}
        
    std::shared_ptr<const Type> type() const override { return type_; }
    std::string toString() const override { return name_; }
    std::string name() const { return name_; }

private:
    std::shared_ptr<const Type> type_;
    std::string name_;
};

class ConstantTypedExpr : public ITypedExpr {
public:
    ConstantTypedExpr(Variant value) : value_(value) {}
    
    std::shared_ptr<const Type> type() const override { 
        if (value_.kind() == TypeKind::INTEGER) return INTEGER();
        if (value_.kind() == TypeKind::BIGINT) return BIGINT();
        if (value_.kind() == TypeKind::VARCHAR) return VARCHAR();
        return std::make_shared<IntegerType>(); // Default/Unknown
    }
    std::string toString() const override { return value_.toString(); }
    const Variant& value() const { return value_; }
    
private:
    Variant value_;
};

class CallTypedExpr : public ITypedExpr {
public:
    CallTypedExpr(std::shared_ptr<const Type> type, std::vector<TypedExprPtr> inputs, std::string name)
        : type_(type), inputs_(std::move(inputs)), name_(name) {}
        
    std::shared_ptr<const Type> type() const override { return type_; }
    std::string toString() const override { 
        std::string s = name_ + "(";
        for(size_t i=0; i<inputs_.size(); ++i) {
            if(i>0) s+= ",";
            s+= inputs_[i]->toString();
        }
        s+=")";
        return s;
    }
    const std::vector<TypedExprPtr>& inputs() const { return inputs_; }
    const std::string& name() const { return name_; }

private:
    std::shared_ptr<const Type> type_;
    std::vector<TypedExprPtr> inputs_;
    std::string name_;
};

class Expressions {
public:
    static TypedExprPtr inferTypes(const TypedExprPtr& untyped, const std::shared_ptr<const RowType>& rowType, memory::MemoryPool* pool) {
        if (auto c = std::dynamic_pointer_cast<ConstantTypedExpr>(untyped)) {
            return c; // Constants are already typed
        }
        
        if (auto f = std::dynamic_pointer_cast<FieldAccessTypedExpr>(untyped)) {
            // Lookup field
            const auto& names = rowType->names();
            for (size_t i = 0; i < names.size(); ++i) {
                if (names[i] == f->name()) {
                    return std::make_shared<FieldAccessTypedExpr>(rowType->childAt(i), f->name());
                }
            }
            throw std::runtime_error("Field not found: " + f->name());
        }
        
        if (auto call = std::dynamic_pointer_cast<CallTypedExpr>(untyped)) {
            std::vector<TypedExprPtr> typedInputs;
            for (auto& in : call->inputs()) {
                typedInputs.push_back(inferTypes(in, rowType, pool));
            }
            
            std::string name = call->name();
            std::shared_ptr<const Type> type;
            
            // Simple type inference
            if (name == "plus" || name == "minus" || name == "multiply" || name == "mod" || name == "divide") {
                type = BIGINT(); 
            } else if (name == "concat" || name == "upper" || name == "substr") {
                type = VARCHAR();
            } else if (name == "eq" || name == "neq" || name == "lt" || name == "gt" || name == "lte" || name == "gte") {
                type = INTEGER();
            } else {
                throw std::runtime_error("Unknown function in inference: " + name);
            }
            
            return std::make_shared<CallTypedExpr>(type, std::move(typedInputs), name);
        }
        
        throw std::runtime_error("Unknown expr type in inference");
    }
};

}
