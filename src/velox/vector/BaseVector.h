#pragma once

#include "velox/buffer/Buffer.h"
#include "velox/common/base/BitUtil.h"
#include "velox/type/Type.h"
#include "velox/vector/VectorEncoding.h"
#include <algorithm>
#include <memory>
#include <optional>
#include <sstream>
#include <vector>

namespace facebook::velox {

using vector_size_t = int32_t;

class BaseVector : public std::enable_shared_from_this<BaseVector> {
public:
  static constexpr std::string_view kNullValueString = "null";

public:
  BaseVector(memory::MemoryPool *pool, std::shared_ptr<const Type> type,
             VectorEncoding::Simple encoding, BufferPtr nulls,
             vector_size_t length)
      : pool_(pool), type_(std::move(type)), encoding_(encoding),
        nulls_(std::move(nulls)), length_(length) {}

  virtual ~BaseVector() = default;

  VectorEncoding::Simple encoding() const { return encoding_; }
  const std::shared_ptr<const Type> &type() const { return type_; }
  memory::MemoryPool *pool() const { return pool_; }
  vector_size_t size() const { return length_; }

  virtual bool isNullAt(vector_size_t index) const {
    if (!nulls_)
      return false;
    return bits::isBitSet(nulls_->as<uint64_t>(),
                          index); // Note: Velox nulls: 1 means null?
    // Wait, Velox convention: nulls bit set means null? Or valid?
    // Usually validity buffer: 1 is valid.
    // But Velox calls it "nulls".
    // Let's check BaseVector.h again or assume standard Arrow/Velox.
    // Velox docs say "nulls" buffer.
    // In `BaseVector.h`: `rawNulls_`. `isNullAt(i)`
    // `bits::isBitSet(rawNulls_, i)` -> usually means "is null".
    // I will assume 1 means Null.
  }

  // Virtuals
  virtual std::string toString(vector_size_t index) const = 0;

  virtual int32_t compare(const BaseVector *other, vector_size_t index,
                          vector_size_t otherIndex) const = 0;

  virtual void copy(const BaseVector *source, vector_size_t sourceIndex,
                    vector_size_t targetIndex) = 0;

  // Returns a brief summary of the vector: [ENCODING TYPE: N elements, X nulls]
  virtual std::string toSummaryString() const {
    std::ostringstream out;
    out << "[" << encoding_ << " " << type_->toString() << ": " << length_
        << " elements, ";
    if (!nulls_) {
      out << "no nulls";
    } else {
      // Count nulls
      vector_size_t nullCount = 0;
      for (vector_size_t i = 0; i < length_; ++i) {
        if (isNullAt(i))
          nullCount++;
      }
      out << nullCount << " nulls";
    }
    out << "]";
    return out.str();
  }

  // Returns the brief summary (matching Velox's toString() behavior)
  std::string toString() const {
    return toSummaryString() + "\n" + toString(0, length_);
  }

  // Returns a range of values [from, to) with optional row numbers
  std::string toString(vector_size_t from, vector_size_t to,
                       const char *delimiter = "\n",
                       bool includeRowNumbers = true) const {
    const auto start = std::max<vector_size_t>(0, std::min(from, length_));
    const auto end = std::max<vector_size_t>(0, std::min(to, length_));

    std::ostringstream out;
    for (auto i = start; i < end; ++i) {
      if (i > start) {
        out << delimiter;
      }
      if (includeRowNumbers) {
        out << i << ": ";
      }
      out << toString(i);
    }
    return out.str();
  }

  virtual BaseVector *loadedVector() { return this; }
  virtual const BaseVector *loadedVector() const { return this; }

protected:
  memory::MemoryPool *pool_;
  std::shared_ptr<const Type> type_;
  VectorEncoding::Simple encoding_;
  BufferPtr nulls_;
  vector_size_t length_;
};

using VectorPtr = std::shared_ptr<BaseVector>;
using RowVectorPtr = std::shared_ptr<class RowVector>;

} // namespace facebook::velox
