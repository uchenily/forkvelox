#pragma once

#include "velox/common/base/Exceptions.h"
#include <algorithm>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

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
  FUNCTION,
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
    return kind_ == TypeKind::INTEGER ||
           kind_ == TypeKind::BIGINT; // Simplified
  }

  bool isVarchar() const { return kind_ == TypeKind::VARCHAR; }

  bool isRow() const { return kind_ == TypeKind::ROW; }

  virtual bool equivalent(const Type &other) const {
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
      : Type(TypeKind::ROW), names_(std::move(names)),
        children_(std::move(children)) {
    VELOX_CHECK_EQ(names_.size(), children_.size());
  }

  std::string toString() const override {
    std::string s = "ROW(";
    for (size_t i = 0; i < names_.size(); ++i) {
      if (i > 0)
        s += ", ";
      s += names_[i];
      s += " ";
      s += children_[i]->toString();
    }
    s += ")";
    return s;
  }

  size_t size() const { return children_.size(); }
  const std::vector<TypePtr> &children() const { return children_; }
  const std::vector<std::string> &names() const { return names_; }
  const TypePtr &childAt(uint32_t idx) const { return children_.at(idx); }
  const std::string &nameOf(uint32_t idx) const { return names_.at(idx); }

  bool equivalent(const Type &other) const override {
    if (!Type::equivalent(other))
      return false;
    const auto *otherRow = static_cast<const RowType *>(&other);
    if (size() != otherRow->size())
      return false;
    for (size_t i = 0; i < size(); ++i) {
      if (!children_[i]->equivalent(*otherRow->children_[i]))
        return false;
    }
    return true;
  }

private:
  std::vector<std::string> names_;
  std::vector<TypePtr> children_;
};

class ArrayType : public Type {
public:
  explicit ArrayType(TypePtr elementType)
      : Type(TypeKind::ARRAY), elementType_(std::move(elementType)) {}

  const TypePtr &elementType() const { return elementType_; }

  std::string toString() const override {
    return "ARRAY<" + elementType_->toString() + ">";
  }

  bool equivalent(const Type &other) const override {
    if (!Type::equivalent(other))
      return false;
    const auto *otherArray = static_cast<const ArrayType *>(&other);
    return elementType_->equivalent(*otherArray->elementType_);
  }

private:
  TypePtr elementType_;
};

class FunctionType : public Type {
public:
  FunctionType(std::vector<TypePtr> argumentTypes, TypePtr returnType)
      : Type(TypeKind::FUNCTION), argumentTypes_(std::move(argumentTypes)),
        returnType_(std::move(returnType)) {}

  const std::vector<TypePtr> &argumentTypes() const { return argumentTypes_; }
  const TypePtr &returnType() const { return returnType_; }

  std::string toString() const override {
    std::string s = "FUNCTION(";
    for (size_t i = 0; i < argumentTypes_.size(); ++i) {
      if (i > 0)
        s += ", ";
      s += argumentTypes_[i]->toString();
    }
    s += ") -> ";
    s += returnType_->toString();
    return s;
  }

  bool equivalent(const Type &other) const override {
    if (!Type::equivalent(other))
      return false;
    const auto *otherFunc = static_cast<const FunctionType *>(&other);
    if (argumentTypes_.size() != otherFunc->argumentTypes_.size())
      return false;
    for (size_t i = 0; i < argumentTypes_.size(); ++i) {
      if (!argumentTypes_[i]->equivalent(*otherFunc->argumentTypes_[i]))
        return false;
    }
    return returnType_->equivalent(*otherFunc->returnType_);
  }

private:
  std::vector<TypePtr> argumentTypes_;
  TypePtr returnType_;
};

class UnknownType : public Type {
public:
  UnknownType() : Type(TypeKind::UNKNOWN) {}
  std::string toString() const override { return "UNKNOWN"; }
};

using RowTypePtr = std::shared_ptr<const RowType>;

inline std::shared_ptr<const Type> INTEGER() {
  return std::make_shared<IntegerType>();
}
inline std::shared_ptr<const Type> BIGINT() {
  return std::make_shared<BigIntType>();
}
inline std::shared_ptr<const Type> VARCHAR() {
  return std::make_shared<VarcharType>();
}
inline std::shared_ptr<const Type> UNKNOWN() {
  return std::make_shared<UnknownType>();
}

inline std::shared_ptr<const RowType> ROW(std::vector<std::string> names,
                                          std::vector<TypePtr> types) {
  return std::make_shared<RowType>(std::move(names), std::move(types));
}

inline std::shared_ptr<const ArrayType> ARRAY(TypePtr elementType) {
  return std::make_shared<ArrayType>(std::move(elementType));
}

inline std::shared_ptr<const FunctionType>
FUNCTION(std::vector<TypePtr> argumentTypes, TypePtr returnType) {
  return std::make_shared<FunctionType>(std::move(argumentTypes),
                                        std::move(returnType));
}

inline std::shared_ptr<const RowType>
asRowType(const std::shared_ptr<const Type> &type) {
  return std::dynamic_pointer_cast<const RowType>(type);
}

/// Helper function to create a string representation of a list of elements,
/// truncating if the list is too long.
/// @param size Total number of elements.
/// @param stringifyElement Function to call to append individual elements.
/// Will be called up to 'limit' times.
/// @param limit Maximum number of elements to include in the result.
inline std::string stringifyTruncatedElementList(
    size_t size,
    const std::function<void(std::stringstream &, size_t)> &stringifyElement,
    size_t limit = 5) {
  if (size == 0) {
    return "<empty>";
  }

  const size_t limitedSize = std::min(size, limit);

  std::stringstream out;
  out << "{";
  for (size_t i = 0; i < limitedSize; ++i) {
    if (i > 0) {
      out << ", ";
    }
    stringifyElement(out, i);
  }

  if (size > limitedSize) {
    out << ", ..." << (size - limitedSize) << " more";
  }
  out << "}";
  return out.str();
}

} // namespace facebook::velox
