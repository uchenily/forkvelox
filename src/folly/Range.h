#pragma once
#include <cstddef>
#include <string_view>
#include <vector>

namespace folly {

template <typename Iter> class Range {
public:
  Range() : start_(), end_() {}
  Range(Iter start, Iter end) : start_(start), end_(end) {}
  Range(Iter start, size_t size) : start_(start), end_(start + size) {}

  Iter data() const { return start_; }
  size_t size() const { return end_ - start_; }
  bool empty() const { return start_ == end_; }

  Iter begin() const { return start_; }
  Iter end() const { return end_; }

  auto &operator[](size_t i) const { return start_[i]; }

private:
  Iter start_;
  Iter end_;
};

using StringPiece = Range<const char *>;

} // namespace folly