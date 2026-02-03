#pragma once
#include <thread>

namespace folly {
inline unsigned int hardware_concurrency() {
  return std::thread::hardware_concurrency();
}
} // namespace folly