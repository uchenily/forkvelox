#pragma once
#include "velox/vector/BaseVector.h"
#include "velox/expression/EvalCtx.h"

namespace facebook::velox::exec {

class VectorFunction {
public:
    virtual ~VectorFunction() = default;
    virtual void apply(const SelectivityVector& rows, 
                       std::vector<VectorPtr>& args, 
                       const TypePtr& outputType,
                       EvalCtx& context, 
                       VectorPtr& result) const = 0;
};

}
