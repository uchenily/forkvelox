#pragma once

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <iostream>
#include <sstream>

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
    ROW,
    ARRAY,
    MAP,
    UNKNOWN
};

class Type {
public:
    virtual ~Type() = default;
    virtual TypeKind kind() const = 0;
    virtual std::string toString() const = 0;
    
    bool isRow() const { return kind() == TypeKind::ROW; }
    
    virtual bool operator==(const Type& other) const {
        return kind() == other.kind();
    }
};

using TypePtr = std::shared_ptr<const Type>;

class IntegerType : public Type {
public:
    TypeKind kind() const override { return TypeKind::INTEGER; }
    std::string toString() const override { return "INTEGER"; }
    static TypePtr create() { return std::make_shared<IntegerType>(); }
};

class BigIntType : public Type {
public:
    TypeKind kind() const override { return TypeKind::BIGINT; }
    std::string toString() const override { return "BIGINT"; }
    static TypePtr create() { return std::make_shared<BigIntType>(); }
};

class VarcharType : public Type {
public:
    TypeKind kind() const override { return TypeKind::VARCHAR; }
    std::string toString() const override { return "VARCHAR"; }
    static TypePtr create() { return std::make_shared<VarcharType>(); }
};

class RowType : public Type {
public:
    RowType(std::vector<std::string> names, std::vector<TypePtr> children)
        : names_(std::move(names)), children_(std::move(children)) {}

    TypeKind kind() const override { return TypeKind::ROW; }
    std::string toString() const override {
        std::stringstream ss;
        ss << "ROW(";
        for (size_t i = 0; i < names_.size(); ++i) {
            if (i > 0) ss << ", ";
            ss << names_[i] << " " << children_[i]->toString();
        }
        ss << ")";
        return ss.str();
    }

    const std::vector<std::string>& names() const { return names_; }
    const std::vector<TypePtr>& children() const { return children_; }
    size_t size() const { return children_.size(); }
    const TypePtr& childAt(uint32_t idx) const { return children_.at(idx); }
    const std::string& nameOf(uint32_t idx) const { return names_.at(idx); }

    static TypePtr create(std::vector<std::string> names, std::vector<TypePtr> children) {
        return std::make_shared<RowType>(std::move(names), std::move(children));
    }

private:
    std::vector<std::string> names_;
    std::vector<TypePtr> children_;
};

using RowTypePtr = std::shared_ptr<const RowType>;

// Simplified TypeTraits
template <TypeKind KIND> struct TypeTraits {};

template <> struct TypeTraits<TypeKind::INTEGER> { using NativeType = int32_t; };
template <> struct TypeTraits<TypeKind::BIGINT> { using NativeType = int64_t; };
template <> struct TypeTraits<TypeKind::VARCHAR> { using NativeType = std::string; }; // Simplified, Velox uses StringView

} // namespace facebook::velox
