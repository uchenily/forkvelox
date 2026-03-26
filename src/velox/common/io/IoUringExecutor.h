#pragma once

#include <functional>

struct io_uring_sqe;

namespace facebook::velox::io {

class IoUringExecutor {
 public:
  using PrepareFn = std::function<void(io_uring_sqe*)>;
  using CompletionFn = std::function<void(int)>;

  static IoUringExecutor& instance();

  IoUringExecutor();
  ~IoUringExecutor();

  IoUringExecutor(const IoUringExecutor&) = delete;
  IoUringExecutor& operator=(const IoUringExecutor&) = delete;

  void submit(PrepareFn prepare, CompletionFn complete);

 private:
  struct Impl;
  Impl* impl_;
};

} // namespace facebook::velox::io
