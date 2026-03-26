#pragma once

#include <condition_variable>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace facebook::velox::async {

class AsyncEvent {
 public:
  AsyncEvent() : state_(std::make_shared<State>()) {}

  bool isReady() const {
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->ready;
  }

  void wait() const {
    std::unique_lock<std::mutex> lock(state_->mutex);
    state_->cv.wait(lock, [&]() { return state_->ready; });
  }

  void notify() {
    std::vector<std::function<void()>> callbacks;
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      if (state_->ready) {
        return;
      }
      state_->ready = true;
      callbacks.swap(state_->callbacks);
    }
    state_->cv.notify_all();
    for (auto& callback : callbacks) {
      callback();
    }
  }

  void subscribe(std::function<void()> callback) const {
    bool runNow = false;
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      if (state_->ready) {
        runNow = true;
      } else {
        state_->callbacks.push_back(std::move(callback));
      }
    }
    if (runNow) {
      callback();
    }
  }

 private:
  struct State {
    std::mutex mutex;
    std::condition_variable cv;
    bool ready{false};
    std::vector<std::function<void()>> callbacks;
  };

  std::shared_ptr<State> state_;
};

template <typename T>
class AsyncValue {
 public:
  AsyncValue() : state_(std::make_shared<State>()) {}

  explicit AsyncValue(T value) : AsyncValue() {
    setValue(std::move(value));
  }

  void setValue(T value) {
    std::vector<std::function<void()>> callbacks;
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      if (state_->ready) {
        return;
      }
      state_->value = std::move(value);
      state_->ready = true;
      callbacks.swap(state_->callbacks);
    }
    state_->cv.notify_all();
    for (auto& callback : callbacks) {
      callback();
    }
  }

  void setError(std::exception_ptr error) {
    std::vector<std::function<void()>> callbacks;
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      if (state_->ready) {
        return;
      }
      state_->error = std::move(error);
      state_->ready = true;
      callbacks.swap(state_->callbacks);
    }
    state_->cv.notify_all();
    for (auto& callback : callbacks) {
      callback();
    }
  }

  T get() const {
    std::unique_lock<std::mutex> lock(state_->mutex);
    state_->cv.wait(lock, [&]() { return state_->ready; });
    if (state_->error) {
      std::rethrow_exception(state_->error);
    }
    return *state_->value;
  }

  void subscribe(std::function<void()> callback) const {
    bool runNow = false;
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      if (state_->ready) {
        runNow = true;
      } else {
        state_->callbacks.push_back(std::move(callback));
      }
    }
    if (runNow) {
      callback();
    }
  }

 private:
  struct State {
    std::mutex mutex;
    std::condition_variable cv;
    bool ready{false};
    std::optional<T> value;
    std::exception_ptr error;
    std::vector<std::function<void()>> callbacks;
  };

  std::shared_ptr<State> state_;
};

} // namespace facebook::velox::async
