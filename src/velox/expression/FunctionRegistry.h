#pragma once
#include "velox/expression/VectorFunction.h"
#include <memory>
#include <string>

namespace facebook::velox::exec {

void registerFunction(const std::string &name, std::shared_ptr<VectorFunction> func);

std::shared_ptr<VectorFunction> getVectorFunction(const std::string &name);

} // namespace facebook::velox::exec
