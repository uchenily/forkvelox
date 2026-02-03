#pragma once

#include "velox/core/ExecCtx.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

class ExprSet;

class EvalCtx {
public:
  EvalCtx(core::ExecCtx *execCtx, ExprSet *exprSet, const RowVector *row)
      : execCtx_(execCtx), exprSet_(exprSet), row_(row) {}

  const RowVector *row() const { return row_; }
  memory::MemoryPool *pool() const { return execCtx_->pool(); }
  core::ExecCtx *execCtx() const { return execCtx_; }

private:
  core::ExecCtx *execCtx_;
  ExprSet *exprSet_;
  const RowVector *row_;
};

} // namespace facebook::velox::exec
