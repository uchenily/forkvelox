#pragma once
#include <memory>
#include <string>
#include "velox/type/Type.h"

namespace facebook::velox::core {

class ITypedExpr {
public:
    virtual ~ITypedExpr() = default;
    virtual std::shared_ptr<const Type> type() const = 0;
    virtual std::string toString() const = 0;
};

using TypedExprPtr = std::shared_ptr<ITypedExpr>;

}
