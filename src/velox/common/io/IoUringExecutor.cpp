#include "velox/common/io/IoUringExecutor.h"

#include <liburing.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::io {

struct IoUringExecutor::Impl {
  struct Request {
    CompletionFn complete;
  };

  Impl() {
    const int rc = io_uring_queue_init(256, &ring, 0);
    VELOX_CHECK_EQ(rc, 0, "Failed to initialize io_uring: {}", rc);
    worker = std::thread([this]() { run(); });
  }

  ~Impl() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      stopping = true;
      auto* sqe = io_uring_get_sqe(&ring);
      if (sqe != nullptr) {
        io_uring_prep_nop(sqe);
        io_uring_sqe_set_data(sqe, nullptr);
        io_uring_submit(&ring);
      }
    }
    if (worker.joinable()) {
      worker.join();
    }
    io_uring_queue_exit(&ring);
  }

  void submit(PrepareFn prepare, CompletionFn complete) {
    auto request = std::make_unique<Request>();
    request->complete = std::move(complete);

    std::lock_guard<std::mutex> lock(mutex);
    auto* sqe = io_uring_get_sqe(&ring);
    VELOX_CHECK_NOT_NULL(sqe, "io_uring submission queue is full");
    prepare(sqe);
    io_uring_sqe_set_data(sqe, request.get());
    requests.push_back(std::move(request));
    const int rc = io_uring_submit(&ring);
    VELOX_CHECK_GE(rc, 0, "io_uring_submit failed: {}", rc);
  }

  void run() {
    while (true) {
      io_uring_cqe* cqe = nullptr;
      const int rc = io_uring_wait_cqe(&ring, &cqe);
      if (rc < 0) {
        continue;
      }

      Request* request = static_cast<Request*>(io_uring_cqe_get_data(cqe));
      const int result = cqe->res;
      io_uring_cqe_seen(&ring, cqe);

      std::unique_ptr<Request> owned;
      {
        std::lock_guard<std::mutex> lock(mutex);
        if (request != nullptr) {
          auto it = std::find_if(requests.begin(), requests.end(), [&](const auto& candidate) {
            return candidate.get() == request;
          });
          if (it != requests.end()) {
            owned = std::move(*it);
            requests.erase(it);
          }
        }
        if (stopping && requests.empty() && request == nullptr) {
          break;
        }
      }

      if (owned) {
        owned->complete(result);
      }
    }
  }

  std::mutex mutex;
  io_uring ring{};
  std::thread worker;
  bool stopping{false};
  std::vector<std::unique_ptr<Request>> requests;
};

IoUringExecutor& IoUringExecutor::instance() {
  static IoUringExecutor executor;
  return executor;
}

IoUringExecutor::IoUringExecutor() : impl_(new Impl()) {}

IoUringExecutor::~IoUringExecutor() {
  delete impl_;
}

void IoUringExecutor::submit(PrepareFn prepare, CompletionFn complete) {
  impl_->submit(std::move(prepare), std::move(complete));
}

} // namespace facebook::velox::io
