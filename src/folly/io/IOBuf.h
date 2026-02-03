#pragma once
#include "folly/Range.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

namespace folly {

class IOBuf {
public:
  enum CreateOp { CREATE };

  IOBuf() = default;
  IOBuf(CreateOp, size_t capacity) { data_.resize(capacity); }

  // Minimal implementation for single-buffer IOBuf
  const uint8_t *data() const { return data_.data(); }
  uint8_t *writableData() { return data_.data(); }
  size_t length() const { return data_.size(); }
  size_t size() const { return data_.size(); }

  void append(size_t len) {
    // simplified: assume pre-allocated resize was enough or just do nothing if
    // we use vector logic
    if (len > data_.size())
      data_.resize(len);
  }

  // Chain iterator stub
  struct Iterator {
    const IOBuf *buf;
    bool done;

    Iterator(const IOBuf *b, bool d) : buf(b), done(d) {}

    bool operator!=(const Iterator &other) const { return done != other.done; }
    Iterator &operator++() {
      done = true;
      return *this;
    }
    Range<const uint8_t *> operator*() const {
      return Range<const uint8_t *>(buf->data_.data(), buf->data_.size());
    }
  };

  Iterator begin() const { return Iterator(this, false); }
  Iterator end() const { return Iterator(this, true); }

  uint64_t computeChainDataLength() const { return data_.size(); }

private:
  std::vector<uint8_t> data_;
};

} // namespace folly
