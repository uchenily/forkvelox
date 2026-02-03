#pragma once

#include "velox/vector/BaseVector.h"
#include <sstream>

namespace facebook::velox {

template <typename T>
class SimpleVector : public BaseVector {
public:
  using BaseVector::BaseVector;
  using BaseVector::toString;

  virtual T valueAt(vector_size_t index) const = 0;

  int32_t compare(const BaseVector *other, vector_size_t index, vector_size_t otherIndex) const override {
    // Assume same type for now
    auto *otherVec = static_cast<const SimpleVector<T> *>(other);
    T v1 = valueAt(index);
    T v2 = otherVec->valueAt(otherIndex);
    if (v1 < v2)
      return -1;
    if (v1 > v2)
      return 1;
    return 0;
  }

  void copy(const BaseVector *source, vector_size_t sourceIndex, vector_size_t targetIndex) override {
    auto *srcVec = static_cast<const SimpleVector<T> *>(source);
    // Mutable access needs to be handled in FlatVector mostly, but SimpleVector
    // stores rawValues_ pointer usually Actually FlatVector owns the buffer.
    // SimpleVector just views it. We need a virtual setValue? Or cast to
    // FlatVector. For ForkVelox, assume FlatVector. But this method is in
    // SimpleVector. Let's make it abstract here or cast to FlatVector. Better:
    // implement in FlatVector. But we need it visible here? No, implement in
    // FlatVector.
  }

  std::string toString(vector_size_t index) const override {
    if (isNullAt(index))
      return "null";
    std::stringstream ss;
    ss << valueAt(index);
    return ss.str();
  }
};

} // namespace facebook::velox
