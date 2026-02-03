#pragma once

#include "velox/expression/Expr.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::exec {

class LambdaExpr : public Expr {
public:
  LambdaExpr(RowTypePtr signature, ExprPtr body,
             std::vector<std::string> captures, TypePtr type)
      : Expr(std::move(type), {}, "lambda"), signature_(std::move(signature)),
        body_(std::move(body)), captures_(std::move(captures)) {}

  void eval(const SelectivityVector &rows, EvalCtx &context,
            VectorPtr &result) override;

  const RowTypePtr &signature() const { return signature_; }
  const ExprPtr &body() const { return body_; }
  const std::vector<std::string> &captures() const { return captures_; }

private:
  RowTypePtr signature_;
  ExprPtr body_;
  std::vector<std::string> captures_;
};

} // namespace facebook::velox::exec
