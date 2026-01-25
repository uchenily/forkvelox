#pragma once
#include "velox/core/ITypedExpr.h"
#include "velox/type/Variant.h"
#include <vector>

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

// Parsing helper for demo
class Expressions {
public:
    static TypedExprPtr inferTypes(const TypedExprPtr& untyped, const std::shared_ptr<const RowType>& rowType, memory::MemoryPool* pool) {
        // In this simplified version, untyped is already typed because our Parser will return TypedExpr directly.
        // But Velox uses separate parsing tree. 
        // For now, just return untyped. 
        // Real inferTypes resolves types.
        return untyped;
    }
};

}
