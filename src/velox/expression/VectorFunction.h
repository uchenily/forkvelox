#pragma once
#include "velox/expression/EvalCtx.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {

class VectorFunction {
public:
  virtual ~VectorFunction() = default;
  virtual void apply(const SelectivityVector &rows,
                     std::vector<VectorPtr> &args, const TypePtr &outputType,
                     EvalCtx &context, VectorPtr &result) const = 0;
};

} // namespace facebook::velox::exec
