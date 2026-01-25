#pragma once

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>

#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

class LocalExchangeQueue {
public:
  explicit LocalExchangeQueue(size_t numProducers);

  void enqueue(RowVectorPtr batch);
  bool dequeue(RowVectorPtr& out);
  void producerFinished();

private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<RowVectorPtr> queue_;
  size_t producersRemaining_{0};
};

} // namespace facebook::velox::exec
