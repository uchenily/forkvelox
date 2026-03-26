#pragma once

#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "velox/common/async/Async.h"
#include "velox/exec/BlockingReason.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

class LocalExchangeQueue : public std::enable_shared_from_this<LocalExchangeQueue> {
 public:
  explicit LocalExchangeQueue(std::string id) : id_(std::move(id)) {}

  void setOnStateChange(std::function<void()> onStateChange) {
    onStateChange_ = std::move(onStateChange);
  }

  void addProducer();
  void enqueue(RowVectorPtr batch);
  bool dequeue(RowVectorPtr& batch);
  void producerFinished();
  BlockingReason blockingReason(std::shared_ptr<async::AsyncEvent>* event = nullptr);

  const std::string& id() const {
    return id_;
  }

 private:
  void notify();

  std::string id_;
  mutable std::mutex mutex_;
  std::deque<RowVectorPtr> queue_;
  std::vector<std::shared_ptr<async::AsyncEvent>> waiters_;
  size_t producers_{0};
  size_t finishedProducers_{0};
  std::function<void()> onStateChange_;
};

} // namespace facebook::velox::exec
