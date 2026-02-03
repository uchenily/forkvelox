#pragma once
#include "velox/core/PlanNode.h"
namespace facebook::velox::core {
class PlanFragment {
public:
  PlanFragment(PlanNodePtr root) : root_(root) {}
  PlanNodePtr root() const { return root_; }

private:
  PlanNodePtr root_;
};
} // namespace facebook::velox::core
