#pragma once

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

namespace facebook::velox {

using vector_size_t = int32_t;

class SelectivityVector {
public:
  SelectivityVector(vector_size_t size, bool allSelected = true)
      : size_(size), allSelected_(allSelected) {
    bits_.resize(bits::nwords(size), allSelected ? ~0ULL : 0);
  }

  vector_size_t size() const { return size_; }
  bool isAllSelected() const { return allSelected_; }

  bool isValid(vector_size_t index) const {
    return bits::isBitSet(bits_.data(), index);
  }

  void setValid(vector_size_t index, bool valid) {
    bits::setBit(bits_.data(), index, valid);
    allSelected_ = false; // Pessimistic
  }

  void resize(vector_size_t size, bool value = true) {
    size_ = size;
    bits_.resize(bits::nwords(size), value ? ~0ULL : 0);
    allSelected_ = value; // Simplification
  }

  bool hasSelections() const {
    for (vector_size_t i = 0; i < size_; ++i) {
      if (isValid(i)) {
        return true;
      }
    }
    return false;
  }

  void intersect(const SelectivityVector &other) {
    VELOX_CHECK_EQ(size_, other.size_);
    for (size_t i = 0; i < bits_.size(); ++i) {
      bits_[i] &= other.bits_[i];
    }
    allSelected_ = false;
  }

  bool intersects(const SelectivityVector &other) const {
    VELOX_CHECK_EQ(size_, other.size_);
    for (vector_size_t i = 0; i < size_; ++i) {
      if (isValid(i) && other.isValid(i)) {
        return true;
      }
    }
    return false;
  }

  template <typename Callable> void applyToSelected(Callable func) const {
    for (vector_size_t i = 0; i < size_; ++i) {
      if (isValid(i)) {
        func(i);
      }
    }
  }

  // For test
  std::string toString() const {
    std::stringstream ss;
    ss << "SelectivityVector(size=" << size_ << ", selected=";
    int count = 0;
    for (int i = 0; i < size_; ++i)
      if (isValid(i))
        count++;
    ss << count << ")";
    return ss.str();
  }

  const uint64_t *asRange() const { return bits_.data(); }

private:
  vector_size_t size_;
  std::vector<uint64_t> bits_;
  bool allSelected_;
};

} // namespace facebook::velox
