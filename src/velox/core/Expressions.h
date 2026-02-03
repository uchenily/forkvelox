#pragma once
#include "velox/common/memory/MemoryPool.h"
#include "velox/core/ITypedExpr.h"
#include "velox/type/Variant.h"
#include <algorithm>
#include <stdexcept>
#include <vector>

namespace facebook::velox::core {

class FieldAccessTypedExpr : public ITypedExpr {
public:
  FieldAccessTypedExpr(std::shared_ptr<const Type> type, std::string name) : type_(type), name_(name) {}

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
    if (value_.kind() == TypeKind::INTEGER)
      return INTEGER();
    if (value_.kind() == TypeKind::BIGINT)
      return BIGINT();
    if (value_.kind() == TypeKind::VARCHAR)
      return VARCHAR();
    return std::make_shared<IntegerType>(); // Default/Unknown
  }
  std::string toString() const override { return value_.toString(); }
  const Variant &value() const { return value_; }

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
    for (size_t i = 0; i < inputs_.size(); ++i) {
      if (i > 0)
        s += ",";
      s += inputs_[i]->toString();
    }
    s += ")";
    return s;
  }
  const std::vector<TypedExprPtr> &inputs() const { return inputs_; }
  const std::string &name() const { return name_; }

private:
  std::shared_ptr<const Type> type_;
  std::vector<TypedExprPtr> inputs_;
  std::string name_;
};

class LambdaTypedExpr : public ITypedExpr {
public:
  LambdaTypedExpr(std::shared_ptr<const RowType> signature, TypedExprPtr body)
      : signature_(std::move(signature)), body_(std::move(body)) {}

  std::shared_ptr<const Type> type() const override {
    std::vector<TypePtr> args = signature_->children();
    return FUNCTION(std::move(args), body_->type());
  }

  std::string toString() const override { return "lambda " + signature_->toString() + " -> " + body_->toString(); }

  const std::shared_ptr<const RowType> &signature() const { return signature_; }
  const TypedExprPtr &body() const { return body_; }

private:
  std::shared_ptr<const RowType> signature_;
  TypedExprPtr body_;
};

class Expressions {
public:
  static TypedExprPtr inferTypes(const TypedExprPtr &untyped, const std::shared_ptr<const RowType> &rowType,
                                 memory::MemoryPool *pool, const std::vector<TypePtr> *lambdaInputTypes = nullptr) {
    if (auto c = std::dynamic_pointer_cast<ConstantTypedExpr>(untyped)) {
      return c; // Constants are already typed
    }

    if (auto f = std::dynamic_pointer_cast<FieldAccessTypedExpr>(untyped)) {
      // Lookup field
      const auto &names = rowType->names();
      for (size_t i = 0; i < names.size(); ++i) {
        if (names[i] == f->name()) {
          return std::make_shared<FieldAccessTypedExpr>(rowType->childAt(i), f->name());
        }
      }
      throw std::runtime_error("Field not found: " + f->name());
    }

    if (auto lambda = std::dynamic_pointer_cast<LambdaTypedExpr>(untyped)) {
      VELOX_CHECK_NOT_NULL(lambdaInputTypes, "Lambda input types are required for type inference");
      const auto &names = lambda->signature()->names();
      VELOX_CHECK_EQ(names.size(), lambdaInputTypes->size());
      std::vector<std::string> scopedNames = names;
      std::vector<TypePtr> scopedTypes = *lambdaInputTypes;
      if (rowType) {
        const auto &outerNames = rowType->names();
        for (size_t i = 0; i < outerNames.size(); ++i) {
          if (std::find(scopedNames.begin(), scopedNames.end(), outerNames[i]) == scopedNames.end()) {
            scopedNames.push_back(outerNames[i]);
            scopedTypes.push_back(rowType->childAt(i));
          }
        }
      }
      auto lambdaScope = ROW(std::move(scopedNames), std::move(scopedTypes));
      auto typedBody = inferTypes(lambda->body(), lambdaScope, pool);
      auto lambdaSignature = ROW(std::vector<std::string>(names.begin(), names.end()),
                                 std::vector<TypePtr>(lambdaInputTypes->begin(), lambdaInputTypes->end()));
      return std::make_shared<LambdaTypedExpr>(lambdaSignature, typedBody);
    }

    if (auto call = std::dynamic_pointer_cast<CallTypedExpr>(untyped)) {
      std::vector<TypedExprPtr> typedInputs;
      std::string name = call->name();
      std::shared_ptr<const Type> type;

      // Simple type inference
      if (name == "transform" || name == "filter") {
        VELOX_CHECK_EQ(call->inputs().size(), 2, "{} expects 2 arguments", name);
        typedInputs.push_back(inferTypes(call->inputs()[0], rowType, pool));
        auto arrayType = std::dynamic_pointer_cast<const ArrayType>(typedInputs[0]->type());
        VELOX_CHECK_NOT_NULL(arrayType.get(), "{} expects ARRAY input", name);

        std::vector<TypePtr> lambdaInputs{arrayType->elementType()};
        typedInputs.push_back(inferTypes(call->inputs()[1], rowType, pool, &lambdaInputs));
        auto lambdaExpr = std::dynamic_pointer_cast<LambdaTypedExpr>(typedInputs[1]);
        VELOX_CHECK_NOT_NULL(lambdaExpr.get(), "{} expects lambda argument", name);

        if (name == "transform") {
          type = ARRAY(lambdaExpr->body()->type());
        } else {
          type = typedInputs[0]->type();
        }
      } else {
        for (auto &in : call->inputs()) {
          typedInputs.push_back(inferTypes(in, rowType, pool));
        }
      }

      if (name == "plus" || name == "minus" || name == "multiply" || name == "mod" || name == "divide") {
        type = BIGINT();
      } else if (name == "concat" || name == "upper" || name == "substr") {
        type = VARCHAR();
      } else if (name == "eq" || name == "neq" || name == "lt" || name == "gt" || name == "lte" || name == "gte") {
        type = INTEGER();
      } else if (name == "transform" || name == "filter") {
        // Type already inferred above.
      } else {
        throw std::runtime_error("Unknown function in inference: " + name);
      }

      return std::make_shared<CallTypedExpr>(type, std::move(typedInputs), name);
    }

    throw std::runtime_error("Unknown expr type in inference");
  }
};

} // namespace facebook::velox::core
