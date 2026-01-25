#pragma once

#include <cstring>
#include <string>
#include <string_view>
#include <iostream>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

struct StringView {
  uint32_t size_;
  char prefix_[4];
  union {
    const char* data;
    char inline_[8];
  } value_;

  static constexpr uint32_t kPrefixSize = 4;
  static constexpr uint32_t kInlineSize = 12;

  StringView() : size_(0) {
      std::memset(prefix_, 0, kPrefixSize);
      value_.data = nullptr;
  }

  StringView(const char* data, int32_t len) : size_(len) {
      if (len <= kInlineSize) {
          std::memcpy(prefix_, data, len);
          // Zero out the rest
          if (len < kPrefixSize) {
              std::memset(prefix_ + len, 0, kPrefixSize - len);
              std::memset(value_.inline_, 0, 8);
          } else {
               std::memcpy(value_.inline_, data + kPrefixSize, len - kPrefixSize);
               if (len < kInlineSize) {
                   std::memset(value_.inline_ + (len - kPrefixSize), 0, kInlineSize - len);
               }
          }
      } else {
          std::memcpy(prefix_, data, kPrefixSize);
          value_.data = data;
      }
  }

  StringView(const char* data) : StringView(data, std::strlen(data)) {}
  StringView(const std::string& str) : StringView(str.data(), str.size()) {}
  StringView(std::string_view sv) : StringView(sv.data(), sv.size()) {}

  uint32_t size() const { return size_; }
  bool empty() const { return size_ == 0; }
  
  const char* data() const {
      if (isInline()) {
          return prefix_;
      }
      return value_.data;
  }

  bool isInline() const {
      return size_ <= kInlineSize;
  }
  
  std::string_view str() const {
      return std::string_view(data(), size_);
  }
  
  operator std::string_view() const {
      return str();
  }
  
  operator std::string() const {
      return std::string(data(), size_);
  }

  bool operator==(const StringView& other) const {
      if (size_ != other.size_) return false;
      if (size_ == 0) return true;
      // Compare prefix
      if (std::memcmp(prefix_, other.prefix_, std::min((uint32_t)size_, kPrefixSize)) != 0) return false;
      
      if (isInline()) {
          if (size_ <= kPrefixSize) return true; // Already compared
          return std::memcmp(value_.inline_, other.value_.inline_, size_ - kPrefixSize) == 0;
      } else {
          return std::memcmp(value_.data + kPrefixSize, other.value_.data + kPrefixSize, size_ - kPrefixSize) == 0;
      }
  }
  
  friend std::ostream& operator<<(std::ostream& os, const StringView& sv) {
      os.write(sv.data(), sv.size());
      return os;
  }
};

}
