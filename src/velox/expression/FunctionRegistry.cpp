#include "velox/expression/FunctionRegistry.h"
#include <unordered_map>

namespace facebook::velox::exec {

namespace {
std::unordered_map<std::string, std::shared_ptr<VectorFunction>> &registry() {
  static std::unordered_map<std::string, std::shared_ptr<VectorFunction>> r;
  return r;
}
} // namespace

void registerFunction(const std::string &name,
                      std::shared_ptr<VectorFunction> func) {
  registry()[name] = func;
}

std::shared_ptr<VectorFunction> getVectorFunction(const std::string &name) {
  auto &r = registry();
  auto it = r.find(name);
  if (it != r.end()) {
    return it->second;
  }
  return nullptr;
}

} // namespace facebook::velox::exec
