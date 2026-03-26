#include "velox/core/ExecutionRuntime.h"

#include <atomic>
#include <thread>

#include <exec/async_scope.hpp>
#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

namespace facebook::velox::core {

class ExecutionRuntime::Impl {
 public:
  explicit Impl(size_t threads)
      : pool(threads == 0 ? std::max<size_t>(1, std::thread::hardware_concurrency()) : threads) {}

  void launch(std::function<void()> func) {
    auto work = stdexec::starts_on(
        pool.get_scheduler(),
        stdexec::just() | stdexec::then([fn = std::move(func)]() mutable { fn(); }));
    scope.spawn(std::move(work));
  }

  void join() {
    if (joined.exchange(true)) {
      return;
    }
    stdexec::sync_wait(scope.on_empty());
  }

  exec::static_thread_pool pool;
  exec::async_scope scope;
  std::atomic<bool> joined{false};
};

ExecutionRuntime::ExecutionRuntime(size_t threads) : impl_(std::make_unique<Impl>(threads)) {}

ExecutionRuntime::~ExecutionRuntime() {
  join();
}

void ExecutionRuntime::launch(std::function<void()> func) {
  impl_->launch(std::move(func));
}

void ExecutionRuntime::join() {
  impl_->join();
}

} // namespace facebook::velox::core
