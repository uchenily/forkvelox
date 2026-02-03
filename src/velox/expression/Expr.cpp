#include "velox/expression/Expr.h"
#include "velox/core/Expressions.h"
#include "velox/expression/FunctionRegistry.h"
#include "velox/expression/LambdaExpr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/StringView.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include <algorithm>
#include <cctype>
#include <iomanip>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

namespace facebook::velox::exec {

// Base Expr implementations
std::string Expr::toString(bool recursive) const {
  if (recursive) {
    std::stringstream out;
    out << name_;
    appendInputs(out);
    return out.str();
  }
  return name_;
}

void Expr::appendInputs(std::stringstream &stream) const {
  if (!inputs_.empty()) {
    stream << "(";
    for (auto i = 0; i < inputs_.size(); ++i) {
      if (i > 0) {
        stream << ", ";
      }
      stream << inputs_[i]->toString();
    }
    stream << ")";
  }
}

namespace {

// Helpers
ExprPtr compile(const core::TypedExprPtr &expr);

class ConstantExpr : public Expr {
public:
  ConstantExpr(Variant value)
      : Expr(resolveType(value), {}, value.toString()), value_(value) {}

  void eval(const SelectivityVector &rows, EvalCtx &context,
            VectorPtr &result) override {
    auto pool = context.pool();
    int size = rows.size();

    if (value_.kind() == TypeKind::BIGINT) {
      auto flat = std::make_shared<FlatVector<int64_t>>(
          pool, BIGINT(), nullptr, size,
          AlignedBuffer::allocate(size * sizeof(int64_t), pool));
      int64_t v = value_.value<int64_t>();
      auto *raw = flat->mutableRawValues();
      rows.applyToSelected([&](vector_size_t i) { raw[i] = v; });
      result = flat;
    } else if (value_.kind() == TypeKind::INTEGER) {
      auto flat = std::make_shared<FlatVector<int32_t>>(
          pool, INTEGER(), nullptr, size,
          AlignedBuffer::allocate(size * sizeof(int32_t), pool));
      int64_t v = value_.value<int64_t>();
      auto *raw = flat->mutableRawValues();
      rows.applyToSelected([&](vector_size_t i) { raw[i] = (int32_t)v; });
      result = flat;
    } else {
      VELOX_NYI("Constant type not supported in demo yet");
    }
  }

private:
  static std::shared_ptr<const Type> resolveType(const Variant &v) {
    if (v.kind() == TypeKind::INTEGER)
      return INTEGER();
    if (v.kind() == TypeKind::BIGINT)
      return BIGINT();
    return BIGINT(); // Default
  }
  Variant value_;
};

class FieldReference : public Expr {
public:
  FieldReference(std::string name, std::shared_ptr<const Type> type)
      : Expr(type, {}, name) {}

  void eval(const SelectivityVector &rows, EvalCtx &context,
            VectorPtr &result) override {
    auto row = context.row();
    auto &children = row->children();
    auto rowType = asRowType(row->type());
    auto &names = rowType->names();
    for (size_t i = 0; i < names.size(); ++i) {
      if (names[i] == name_) {
        result = children[i];
        return;
      }
    }
    throw std::runtime_error("Field not found: " + name_);
  }
};

class CallExpr : public Expr {
public:
  CallExpr(std::string name, std::vector<ExprPtr> inputs,
           std::shared_ptr<const Type> type)
      : Expr(type, std::move(inputs), name) {}

  void eval(const SelectivityVector &rows, EvalCtx &context,
            VectorPtr &result) override {
    std::vector<VectorPtr> args;
    for (auto &input : inputs_) {
      VectorPtr res;
      input->eval(rows, context, res);
      args.push_back(res);
    }

    // Dispatch
    auto func = getVectorFunction(name_);
    if (func) {
      func->apply(rows, args, type_, context, result);
    } else {
      VELOX_NYI("Function {}", name_);
    }
  }
};

void collectFieldNames(const core::TypedExprPtr &expr,
                       std::unordered_set<std::string> &fieldNames) {
  if (auto f = std::dynamic_pointer_cast<core::FieldAccessTypedExpr>(expr)) {
    fieldNames.insert(f->name());
    return;
  }
  if (auto call = std::dynamic_pointer_cast<core::CallTypedExpr>(expr)) {
    for (const auto &input : call->inputs()) {
      collectFieldNames(input, fieldNames);
    }
    return;
  }
  if (auto lambda = std::dynamic_pointer_cast<core::LambdaTypedExpr>(expr)) {
    // Do not traverse into nested lambdas.
    (void)lambda;
    return;
  }
}

ExprPtr compile(const core::TypedExprPtr &expr) {
  if (auto c = std::dynamic_pointer_cast<core::ConstantTypedExpr>(expr)) {
    return std::make_shared<ConstantExpr>(c->value());
  }
  if (auto f = std::dynamic_pointer_cast<core::FieldAccessTypedExpr>(expr)) {
    return std::make_shared<FieldReference>(f->name(), f->type());
  }
  if (auto lambda = std::dynamic_pointer_cast<core::LambdaTypedExpr>(expr)) {
    std::unordered_set<std::string> fieldNames;
    collectFieldNames(lambda->body(), fieldNames);
    std::vector<std::string> captures;
    const auto &paramNames = lambda->signature()->names();
    for (const auto &name : fieldNames) {
      if (std::find(paramNames.begin(), paramNames.end(), name) ==
          paramNames.end()) {
        captures.push_back(name);
      }
    }
    auto bodyExpr = compile(lambda->body());
    return std::make_shared<LambdaExpr>(lambda->signature(),
                                        std::move(bodyExpr),
                                        std::move(captures), lambda->type());
  }
  if (auto call = std::dynamic_pointer_cast<core::CallTypedExpr>(expr)) {
    std::vector<ExprPtr> args;
    for (auto &in : call->inputs()) {
      args.push_back(compile(in));
    }
    return std::make_shared<CallExpr>(call->name(), std::move(args),
                                      call->type());
  }
  VELOX_FAIL("Unknown expr type");
}

void printExprTree(
    const exec::Expr &expr, const std::string &indent, bool withStats,
    std::stringstream &out,
    std::unordered_map<const exec::Expr *, uint32_t> &uniqueExprs) {

  auto it = uniqueExprs.find(&expr);
  if (it != uniqueExprs.end()) {
    out << indent << expr.toString(true) << " -> " << expr.type()->toString();
    out << " [CSE #" << it->second << "]" << std::endl;
    return;
  }

  uint32_t id = uniqueExprs.size() + 1;
  uniqueExprs.insert({&expr, id});

  const auto &stats = expr.stats();
  out << indent << expr.toString(false);
  // if (withStats) { ... } // stub stats
  out << " -> " << expr.type()->toString() << " [#" << id << "]" << std::endl;

  auto newIndent = indent + "   ";
  for (const auto &input : expr.inputs()) {
    printExprTree(*input, newIndent, withStats, out, uniqueExprs);
  }
}

} // namespace

ExprSet::ExprSet(std::vector<std::shared_ptr<core::ITypedExpr>> sources,
                 core::ExecCtx *execCtx)
    : sources_(std::move(sources)), execCtx_(execCtx) {
  for (auto &source : sources_) {
    exprs_.push_back(compile(source));
  }
}

ExprSet::~ExprSet() {}

void ExprSet::eval(const SelectivityVector &rows, EvalCtx &context,
                   std::vector<VectorPtr> &result) {
  result.resize(exprs_.size());
  for (size_t i = 0; i < exprs_.size(); ++i) {
    exprs_[i]->eval(rows, context, result[i]);
  }
}

std::string ExprSet::toString(bool compact) const {
  std::unordered_map<const exec::Expr *, uint32_t> uniqueExprs;
  std::stringstream out;
  for (size_t i = 0; i < exprs_.size(); ++i) {
    if (i > 0) {
      out << std::endl;
    }
    if (compact) {
      out << exprs_[i]->toString(true /*recursive*/);
    } else {
      printExprTree(*exprs_[i], "", false /*withStats*/, out, uniqueExprs);
    }
  }
  return out.str();
}

} // namespace facebook::velox::exec
