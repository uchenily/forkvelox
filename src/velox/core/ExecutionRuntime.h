#pragma once

#include <cstddef>
#include <functional>
#include <memory>

namespace facebook::velox::core {

class ExecutionRuntime : public std::enable_shared_from_this<ExecutionRuntime> {
 public:
  explicit ExecutionRuntime(size_t threads = 0);
  ~ExecutionRuntime();

  ExecutionRuntime(const ExecutionRuntime&) = delete;
  ExecutionRuntime& operator=(const ExecutionRuntime&) = delete;

  void launch(std::function<void()> func);
  void join();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace facebook::velox::core
