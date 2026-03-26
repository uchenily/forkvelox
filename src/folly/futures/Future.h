#pragma once

#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <condition_variable>

namespace folly {

template <typename T>
struct FutureState {
  std::mutex mutex;
  std::condition_variable cv;
  bool ready{false};
  std::optional<T> value;
  std::exception_ptr exception;
};

template <typename T>
class SemiFuture {
 public:
  struct Try {
    T value;
    std::exception_ptr exception;
  };

  SemiFuture() : state_(std::make_shared<FutureState<T>>()) {}
  explicit SemiFuture(std::shared_ptr<FutureState<T>> state) : state_(std::move(state)) {}
  explicit SemiFuture(T val) : SemiFuture() {
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->value = std::move(val);
    state_->ready = true;
  }
  explicit SemiFuture(std::exception_ptr e) : SemiFuture() {
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->exception = std::move(e);
    state_->ready = true;
  }

  T get() {
    std::unique_lock<std::mutex> lock(state_->mutex);
    state_->cv.wait(lock, [&]() { return state_->ready; });
    if (state_->exception) {
      std::rethrow_exception(state_->exception);
    }
    return *state_->value;
  }

  Try getTry() {
    std::unique_lock<std::mutex> lock(state_->mutex);
    state_->cv.wait(lock, [&]() { return state_->ready; });
    return {state_->value.value_or(T{}), state_->exception};
  }

 private:
  std::shared_ptr<FutureState<T>> state_;

  template <typename U>
  friend struct Promise;
};

template <typename T>
SemiFuture<T> makeSemiFuture(T val) {
  return SemiFuture<T>(std::move(val));
}

template <typename T>
SemiFuture<T> makeSemiFuture(std::exception e) {
  return SemiFuture<T>(std::make_exception_ptr(e));
}

template <typename T>
struct Promise {
  Promise() : state(std::make_shared<FutureState<T>>()) {}
  explicit Promise(std::shared_ptr<FutureState<T>> sharedState) : state(std::move(sharedState)) {}

  void setValue(T val) {
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->value = std::move(val);
      state->ready = true;
    }
    state->cv.notify_all();
  }

  void setException(std::exception_ptr e) {
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->exception = std::move(e);
      state->ready = true;
    }
    state->cv.notify_all();
  }

  void setTry(typename SemiFuture<T>::Try t) {
    if (t.exception) {
      setException(std::move(t.exception));
      return;
    }
    setValue(std::move(t.value));
  }

  std::shared_ptr<FutureState<T>> state;
};

template <typename T>
std::pair<Promise<T>, SemiFuture<T>> makePromiseContract() {
  auto state = std::make_shared<FutureState<T>>();
  return {Promise<T>(state), SemiFuture<T>(state)};
}

} // namespace folly
