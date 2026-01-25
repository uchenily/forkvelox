#pragma once
#include <string>
#include <memory>
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::exec {

void registerFunction(const std::string& name, std::shared_ptr<VectorFunction> func);

std::shared_ptr<VectorFunction> getVectorFunction(const std::string& name);

}
