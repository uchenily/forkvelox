#include "velox/exec/LocalExchange.h"

namespace facebook::velox::exec {

LocalExchangeQueue::LocalExchangeQueue(size_t numProducers)
    : producersRemaining_(numProducers) {}

void LocalExchangeQueue::enqueue(RowVectorPtr batch) {
  if (!batch) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push_back(std::move(batch));
  }
  cv_.notify_one();
}

bool LocalExchangeQueue::dequeue(RowVectorPtr& out) {
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [&]() {
    return !queue_.empty() || producersRemaining_ == 0;
  });
  if (queue_.empty()) {
    return false;
  }
  out = std::move(queue_.front());
  queue_.pop_front();
  return true;
}

void LocalExchangeQueue::producerFinished() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (producersRemaining_ > 0) {
      --producersRemaining_;
    }
  }
  cv_.notify_all();
}

} // namespace facebook::velox::exec
