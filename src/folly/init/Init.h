#pragma once
#include "folly/Executor.h" // For convenience
namespace folly {
namespace init {
struct Init {
  Init(int *argc, char ***argv, bool removeFlags = true) {}
};
} // namespace init
} // namespace folly
