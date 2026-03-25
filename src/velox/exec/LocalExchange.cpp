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
  std::vector<std::shared_ptr<ContinuePromise>> waiters;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push_back(std::move(batch));
    waiters.swap(waiters_);
  }
  for (auto& waiter : waiters) {
    waiter->set_value();
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
  std::vector<std::shared_ptr<ContinuePromise>> waiters;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ++finishedProducers_;
    waiters.swap(waiters_);
  }
  for (auto& waiter : waiters) {
    waiter->set_value();
  }
  notify();
}

BlockingReason LocalExchangeQueue::blockingReason(ContinueFuture* future) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!queue_.empty() || finishedProducers_ >= producers_) {
    return BlockingReason::kNotBlocked;
  }
  if (future != nullptr) {
    auto waiter = std::make_shared<ContinuePromise>();
    *future = waiter->get_future().share();
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
