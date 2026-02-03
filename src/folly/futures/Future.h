#pragma once
#include <exception>
#include <future>

namespace folly {

template <typename T> class SemiFuture {
public:
  SemiFuture(T val) : val_(val), has_val_(true) {}
  SemiFuture(std::exception_ptr e) : ex_(e), has_val_(false) {}

  // Stub
  T get() {
    if (!has_val_)
      std::rethrow_exception(ex_);
    return val_;
  }

  // Stub for getTry
  struct Try {
    T value;
    std::exception_ptr exception;
  };

  Try getTry() { return {val_, ex_}; }

private:
  T val_;
  std::exception_ptr ex_;
  bool has_val_;
};

template <typename T> SemiFuture<T> makeSemiFuture(T val) {
  return SemiFuture<T>(val);
}

template <typename T> SemiFuture<T> makeSemiFuture(std::exception e) {
  return SemiFuture<T>(std::make_exception_ptr(e));
}

template <typename T> struct Promise {
  void setValue(T val) {}
  void setException(std::exception_ptr e) {}
  void setTry(typename SemiFuture<T>::Try t) {}
};

template <typename T>
std::pair<Promise<T>, SemiFuture<T>> makePromiseContract() {
  return {Promise<T>(), SemiFuture<T>(T{})};
}

} // namespace folly
