#pragma once

#include <condition_variable>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include <stdexec/execution.hpp>

namespace facebook::velox::async {

class AsyncEvent;
template <typename Receiver>
class AsyncEventOperation;
class AsyncEventSender;

template <typename T>
class AsyncValue;
template <typename T, typename Receiver>
class AsyncValueOperation;
template <typename T>
class AsyncValueSender;

class AsyncEvent {
 public:
  using Sender = AsyncEventSender;

  AsyncEvent() : state_(std::make_shared<State>()) {}

  bool isReady() const {
    std::lock_guard<std::mutex> lock(state_->mutex_);
    return state_->ready_;
  }

  void notify() {
    std::vector<std::function<void()>> callbacks;
    {
      std::lock_guard<std::mutex> lock(state_->mutex_);
      if (state_->ready_) {
        return;
      }
      state_->ready_ = true;
      callbacks.swap(state_->callbacks_);
    }
    state_->cv_.notify_all();
    for (auto& callback : callbacks) {
      callback();
    }
  }

  void subscribe(std::function<void()> callback) const {
    bool runNow = false;
    {
      std::lock_guard<std::mutex> lock(state_->mutex_);
      if (state_->ready_) {
        runNow = true;
      } else {
        state_->callbacks_.push_back(std::move(callback));
      }
    }
    if (runNow) {
      callback();
    }
  }

  Sender sender() const;

 private:
  struct State {
    std::mutex mutex_;
    std::condition_variable cv_;
    bool ready_{false};
    std::vector<std::function<void()>> callbacks_;
  };

  std::shared_ptr<State> state_;
};

template <typename T>
class AsyncValue {
 public:
  using Sender = AsyncValueSender<T>;

  AsyncValue() : state_(std::make_shared<State>()) {}

  explicit AsyncValue(T value) : AsyncValue() {
    setValue(std::move(value));
  }

  void setValue(T value) {
    std::vector<std::function<void()>> callbacks;
    {
      std::lock_guard<std::mutex> lock(state_->mutex_);
      if (state_->ready_) {
        return;
      }
      state_->value_ = std::move(value);
      state_->ready_ = true;
      callbacks.swap(state_->callbacks_);
    }
    state_->cv_.notify_all();
    for (auto& callback : callbacks) {
      callback();
    }
  }

  void setError(std::exception_ptr error) {
    std::vector<std::function<void()>> callbacks;
    {
      std::lock_guard<std::mutex> lock(state_->mutex_);
      if (state_->ready_) {
        return;
      }
      state_->error_ = std::move(error);
      state_->ready_ = true;
      callbacks.swap(state_->callbacks_);
    }
    state_->cv_.notify_all();
    for (auto& callback : callbacks) {
      callback();
    }
  }

  T get() const {
    std::unique_lock<std::mutex> lock(state_->mutex_);
    state_->cv_.wait(lock, [&]() { return state_->ready_; });
    if (state_->error_) {
      std::rethrow_exception(state_->error_);
    }
    return *state_->value_;
  }

  void subscribe(std::function<void()> callback) const {
    bool runNow = false;
    {
      std::lock_guard<std::mutex> lock(state_->mutex_);
      if (state_->ready_) {
        runNow = true;
      } else {
        state_->callbacks_.push_back(std::move(callback));
      }
    }
    if (runNow) {
      callback();
    }
  }

  Sender sender() const;

 private:
  struct State {
    std::mutex mutex_;
    std::condition_variable cv_;
    bool ready_{false};
    std::optional<T> value_;
    std::exception_ptr error_;
    std::vector<std::function<void()>> callbacks_;
  };

  std::shared_ptr<State> state_;
};

template <typename Receiver>
class AsyncEventOperation {
 public:
  AsyncEventOperation(AsyncEvent event, Receiver receiver)
      : event_(std::move(event)), receiver_(std::move(receiver)) {}

  void start() & noexcept {
    event_.subscribe([receiver = std::move(receiver_)]() mutable {
      stdexec::set_value(std::move(receiver));
    });
  }

 private:
  AsyncEvent event_;
  Receiver receiver_;
};

class AsyncEventSender {
 public:
  using sender_concept = stdexec::sender_t;
  using completion_signatures = stdexec::completion_signatures<stdexec::set_value_t()>;

  template <typename Receiver>
  auto connect(Receiver receiver) && {
    return AsyncEventOperation<Receiver>{event_, std::move(receiver)};
  }

  explicit AsyncEventSender(AsyncEvent event) : event_(std::move(event)) {}

 private:
  AsyncEvent event_;
};

inline AsyncEvent::Sender AsyncEvent::sender() const {
  return AsyncEventSender{*this};
}

template <typename T, typename Receiver>
class AsyncValueOperation {
 public:
  AsyncValueOperation(AsyncValue<T> value, Receiver receiver)
      : value_(std::move(value)), receiver_(std::move(receiver)) {}

  void start() & noexcept {
    value_.subscribe([receiver = std::move(receiver_), value = value_]() mutable {
      try {
        stdexec::set_value(std::move(receiver), value.get());
      } catch (...) {
        stdexec::set_error(std::move(receiver), std::current_exception());
      }
    });
  }

 private:
  AsyncValue<T> value_;
  Receiver receiver_;
};

template <typename T>
class AsyncValueSender {
 public:
  using sender_concept = stdexec::sender_t;
  using completion_signatures =
      stdexec::completion_signatures<stdexec::set_value_t(T), stdexec::set_error_t(std::exception_ptr)>;

  template <typename Receiver>
  auto connect(Receiver receiver) && {
    return AsyncValueOperation<T, Receiver>{value_, std::move(receiver)};
  }

  explicit AsyncValueSender(AsyncValue<T> value) : value_(std::move(value)) {}

 private:
  AsyncValue<T> value_;
};

template <typename T>
typename AsyncValue<T>::Sender AsyncValue<T>::sender() const {
  return AsyncValueSender<T>{*this};
}

} // namespace facebook::velox::async
