#pragma once

#include "velox/common/base/Exceptions.h"
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>

namespace facebook::velox {

struct StringView {
  uint32_t size_;
  union {
    char inline_[12];
    struct {
      char prefix_[4];
      const char *data_;
    } external_;
  } value_;

  static constexpr uint32_t kPrefixSize = 4;
  static constexpr uint32_t kInlineSize = 12;

  StringView() : size_(0) { std::memset(value_.inline_, 0, kInlineSize); }

  StringView(const char *data, int32_t len) : size_(len) {
    if (len <= kInlineSize) {
      std::memcpy(value_.inline_, data, len);
      if (len < kInlineSize) {
        std::memset(value_.inline_ + len, 0, kInlineSize - len);
      }
    } else {
      std::memcpy(value_.external_.prefix_, data, kPrefixSize);
      value_.external_.data_ = data;
    }
  }

  StringView(const char *data) : StringView(data, std::strlen(data)) {}
  StringView(const std::string &str) : StringView(str.data(), str.size()) {}
  StringView(std::string_view sv) : StringView(sv.data(), sv.size()) {}

  uint32_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  const char *data() const {
    if (isInline()) {
      return value_.inline_;
    }
    return value_.external_.data_;
  }

  bool isInline() const { return size_ <= kInlineSize; }

  std::string_view str() const { return std::string_view(data(), size_); }

  operator std::string_view() const { return str(); }

  operator std::string() const { return std::string(data(), size_); }

  int compare(const StringView &other) const {
    int cmp = std::memcmp(data(), other.data(), std::min(size_, other.size_));
    if (cmp != 0)
      return cmp;
    if (size_ < other.size_)
      return -1;
    if (size_ > other.size_)
      return 1;
    return 0;
  }

  bool operator<(const StringView &other) const { return compare(other) < 0; }

  bool operator>(const StringView &other) const { return compare(other) > 0; }

  bool operator==(const StringView &other) const {
    if (size_ != other.size_)
      return false;
    if (size_ == 0)
      return true;

    // Compare prefix (first 4 bytes)
    // inline_[0..3] aligns with external_.prefix_
    if (std::memcmp(value_.inline_, other.value_.inline_, std::min((uint32_t)size_, kPrefixSize)) != 0)
      return false;

    if (isInline()) {
      if (size_ <= kPrefixSize)
        return true;
      return std::memcmp(value_.inline_ + kPrefixSize, other.value_.inline_ + kPrefixSize, size_ - kPrefixSize) == 0;
    } else {
      return std::memcmp(value_.external_.data_ + kPrefixSize, other.value_.external_.data_ + kPrefixSize,
                         size_ - kPrefixSize) == 0;
    }
  }

  friend std::ostream &operator<<(std::ostream &os, const StringView &sv) {
    os.write(sv.data(), sv.size());
    return os;
  }
};

} // namespace facebook::velox
