#pragma once

#include <chrono>
#include <cstdint>
#include <condition_variable>
#include <future>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace facebook::velox::exec {

class ContinueEvent;

class ContinueFuture {
 public:
  ContinueFuture() = default;
  explicit ContinueFuture(std::shared_ptr<ContinueEvent> event) : event_(std::move(event)) {}

  bool valid() const;
  ContinueFuture share() const {
    return *this;
  }
  void wait() const;
  std::future_status wait_for(std::chrono::milliseconds timeout) const;
  void subscribe(std::function<void()> callback) const;

 private:
  std::shared_ptr<ContinueEvent> event_;
};

class ContinuePromise {
 public:
  ContinuePromise();

  ContinueFuture get_future() const;
  void set_value();

 private:
  std::shared_ptr<ContinueEvent> event_;
};

class ContinueEvent {
 public:
  bool ready() const;
  void wait() const;
  std::future_status wait_for(std::chrono::milliseconds timeout) const;
  void subscribe(std::function<void()> callback);
  void notify();

 private:
  mutable std::mutex mutex_;
  mutable std::condition_variable cv_;
  bool ready_{false};
  std::vector<std::function<void()>> callbacks_;
};

inline ContinuePromise::ContinuePromise() : event_(std::make_shared<ContinueEvent>()) {}

inline ContinueFuture ContinuePromise::get_future() const {
  return ContinueFuture{event_};
}

inline void ContinuePromise::set_value() {
  event_->notify();
}

inline bool ContinueFuture::valid() const {
  return static_cast<bool>(event_);
}

inline void ContinueFuture::wait() const {
  if (event_) {
    event_->wait();
  }
}

inline std::future_status ContinueFuture::wait_for(std::chrono::milliseconds timeout) const {
  if (!event_) {
    return std::future_status::ready;
  }
  return event_->wait_for(timeout);
}

inline void ContinueFuture::subscribe(std::function<void()> callback) const {
  if (!event_) {
    callback();
    return;
  }
  event_->subscribe(std::move(callback));
}

inline bool ContinueEvent::ready() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return ready_;
}

inline void ContinueEvent::wait() const {
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [&]() { return ready_; });
}

inline std::future_status ContinueEvent::wait_for(std::chrono::milliseconds timeout) const {
  std::unique_lock<std::mutex> lock(mutex_);
  if (cv_.wait_for(lock, timeout, [&]() { return ready_; })) {
    return std::future_status::ready;
  }
  return std::future_status::timeout;
}

inline void ContinueEvent::subscribe(std::function<void()> callback) {
  bool runNow = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ready_) {
      runNow = true;
    } else {
      callbacks_.push_back(std::move(callback));
    }
  }
  if (runNow) {
    callback();
  }
}

inline void ContinueEvent::notify() {
  std::vector<std::function<void()>> callbacks;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ready_) {
      return;
    }
    ready_ = true;
    callbacks.swap(callbacks_);
  }
  cv_.notify_all();
  for (auto& callback : callbacks) {
    callback();
  }
}

enum class BlockingReason : uint8_t {
  kNotBlocked = 0,
  kWaitForSplit,
  kWaitForProducer,
  kWaitForJoinBuild,
  kYield,
  kCancelled,
};

const char* blockingReasonName(BlockingReason reason);

} // namespace facebook::velox::exec
