#pragma once

#include <string>
#include <vector>
#include <memory>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

enum class TypeKind {
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    REAL,
    DOUBLE,
    VARCHAR,
    VARBINARY,
    TIMESTAMP,
    ARRAY,
    MAP,
    ROW,
    UNKNOWN,
    INVALID
};

class Type {
public:
    Type(TypeKind kind) : kind_(kind) {}
    virtual ~Type() = default;

    TypeKind kind() const { return kind_; }
    
    virtual std::string toString() const = 0;
    
    bool isInteger() const {
        return kind_ == TypeKind::INTEGER || kind_ == TypeKind::BIGINT; // Simplified
    }
    
    bool isVarchar() const {
        return kind_ == TypeKind::VARCHAR;
    }

    bool isRow() const {
        return kind_ == TypeKind::ROW;
    }
    
    virtual bool equivalent(const Type& other) const {
        return kind_ == other.kind_;
    }
    
private:
    TypeKind kind_;
};

using TypePtr = std::shared_ptr<const Type>;

class IntegerType : public Type {
public:
    IntegerType() : Type(TypeKind::INTEGER) {}
    std::string toString() const override { return "INTEGER"; }
};

class BigIntType : public Type {
public:
    BigIntType() : Type(TypeKind::BIGINT) {}
    std::string toString() const override { return "BIGINT"; }
};

class VarcharType : public Type {
public:
    VarcharType() : Type(TypeKind::VARCHAR) {}
    std::string toString() const override { return "VARCHAR"; }
};

class RowType : public Type {
public:
    RowType(std::vector<std::string> names, std::vector<TypePtr> children)
        : Type(TypeKind::ROW), names_(std::move(names)), children_(std::move(children)) {
        VELOX_CHECK_EQ(names_.size(), children_.size());
    }

    std::string toString() const override {
        std::string s = "ROW(";
        for (size_t i = 0; i < names_.size(); ++i) {
            if (i > 0) s += ", ";
            s += names_[i];
            s += " ";
            s += children_[i]->toString();
        }
        s += ")";
        return s;
    }
    
    size_t size() const { return children_.size(); }
    const std::vector<TypePtr>& children() const { return children_; }
    const std::vector<std::string>& names() const { return names_; }
    const TypePtr& childAt(uint32_t idx) const { return children_.at(idx); }
    const std::string& nameOf(uint32_t idx) const { return names_.at(idx); }
    
    bool equivalent(const Type& other) const override {
        if (!Type::equivalent(other)) return false;
        const auto* otherRow = static_cast<const RowType*>(&other);
        if (size() != otherRow->size()) return false;
        for (size_t i = 0; i < size(); ++i) {
            if (!children_[i]->equivalent(*otherRow->children_[i])) return false;
             // Names usually don't matter for equivalence in some systems, but for exact match yes.
             // Velox typically checks children types.
        }
        return true;
    }

private:
    std::vector<std::string> names_;
    std::vector<TypePtr> children_;
};

using RowTypePtr = std::shared_ptr<const RowType>;

inline std::shared_ptr<const Type> INTEGER() { return std::make_shared<IntegerType>(); }
inline std::shared_ptr<const Type> BIGINT() { return std::make_shared<BigIntType>(); }
inline std::shared_ptr<const Type> VARCHAR() { return std::make_shared<VarcharType>(); }
inline std::shared_ptr<const RowType> ROW(std::vector<std::string> names, std::vector<TypePtr> types) {
    return std::make_shared<RowType>(std::move(names), std::move(types));
}

}
