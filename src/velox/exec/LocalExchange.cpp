#include "velox/exec/LocalExchange.h"

namespace facebook::velox::exec {

void LocalExchangeQueue::addProducer() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ++producers_;
  }
  notify();
}

void LocalExchangeQueue::enqueue(RowVectorPtr batch) {
  if (!batch) {
    return;
  }
  std::vector<std::shared_ptr<async::AsyncEvent>> waiters;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push_back(std::move(batch));
    waiters.swap(waiters_);
  }
  for (auto& waiter : waiters) {
    waiter->notify();
  }
  notify();
}

bool LocalExchangeQueue::dequeue(RowVectorPtr& batch) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!queue_.empty()) {
    batch = std::move(queue_.front());
    queue_.pop_front();
    return true;
  }
  batch = nullptr;
  return false;
}

void LocalExchangeQueue::producerFinished() {
  std::vector<std::shared_ptr<async::AsyncEvent>> waiters;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ++finishedProducers_;
    waiters.swap(waiters_);
  }
  for (auto& waiter : waiters) {
    waiter->notify();
  }
  notify();
}

BlockingReason LocalExchangeQueue::pendingReason(std::shared_ptr<async::AsyncEvent>* event) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!queue_.empty() || finishedProducers_ >= producers_) {
    return BlockingReason::kNotBlocked;
  }
  if (event != nullptr) {
    auto waiter = std::make_shared<async::AsyncEvent>();
    *event = waiter;
    waiters_.push_back(waiter);
  }
  return BlockingReason::kWaitForProducer;
}

void LocalExchangeQueue::notify() {
  if (onStateChange_) {
    onStateChange_();
  }
}

} // namespace facebook::velox::exec
