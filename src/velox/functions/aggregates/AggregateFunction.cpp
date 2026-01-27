#include "velox/functions/aggregates/AggregateFunction.h"

#include <unordered_map>

namespace facebook::velox::aggregate {

namespace {
std::unordered_map<std::string, std::shared_ptr<AggregateFunction>>& registry() {
  static std::unordered_map<std::string, std::shared_ptr<AggregateFunction>> r;
  return r;
}
}

void registerAggregateFunction(
    const std::string& name,
    std::shared_ptr<AggregateFunction> func) {
  registry()[name] = std::move(func);
}

std::shared_ptr<AggregateFunction> getAggregateFunction(
    const std::string& name) {
  auto& r = registry();
  auto it = r.find(name);
  if (it != r.end()) {
    return it->second;
  }
  return nullptr;
}

}
