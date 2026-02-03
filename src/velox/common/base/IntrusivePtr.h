#pragma once

#include <atomic>
#include <utility>

namespace facebook::velox {

template <typename T>
class IntrusivePtr {
public:
  IntrusivePtr() : ptr_(nullptr) {}
  IntrusivePtr(T *ptr) : ptr_(ptr) {
    if (ptr_)
      ptr_->addRef();
  }
  IntrusivePtr(const IntrusivePtr &other) : ptr_(other.ptr_) {
    if (ptr_)
      ptr_->addRef();
  }
  IntrusivePtr(IntrusivePtr &&other) noexcept : ptr_(other.ptr_) { other.ptr_ = nullptr; }
  ~IntrusivePtr() {
    if (ptr_)
      ptr_->release();
  }

  IntrusivePtr &operator=(const IntrusivePtr &other) {
    if (this != &other) {
      // Add ref first in case other.ptr_ == ptr_
      T *old = ptr_;
      ptr_ = other.ptr_;
      if (ptr_)
        ptr_->addRef();
      if (old)
        old->release();
    }
    return *this;
  }

  IntrusivePtr &operator=(IntrusivePtr &&other) noexcept {
    if (this != &other) {
      if (ptr_)
        ptr_->release();
      ptr_ = other.ptr_;
      other.ptr_ = nullptr;
    }
    return *this;
  }

  T *get() const { return ptr_; }
  T *operator->() const { return ptr_; }
  T &operator*() const { return *ptr_; }
  explicit operator bool() const { return ptr_ != nullptr; }

  bool operator==(const IntrusivePtr &other) const { return ptr_ == other.ptr_; }
  bool operator!=(const IntrusivePtr &other) const { return ptr_ != other.ptr_; }

  void reset(T *ptr = nullptr) {
    if (ptr_ != ptr) {
      T *old = ptr_;
      ptr_ = ptr;
      if (ptr_)
        ptr_->addRef();
      if (old)
        old->release();
    }
  }

private:
  T *ptr_;
};

} // namespace facebook::velox
