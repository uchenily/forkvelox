#pragma once

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox {

class ArrayVector : public BaseVector {
public:
  ArrayVector(memory::MemoryPool *pool, std::shared_ptr<const Type> type,
              BufferPtr nulls, vector_size_t length, BufferPtr offsets,
              BufferPtr sizes, VectorPtr elements)
      : BaseVector(pool, type, VectorEncoding::Simple::ARRAY, std::move(nulls),
                   length),
        offsets_(std::move(offsets)), sizes_(std::move(sizes)),
        elements_(std::move(elements)) {}

  const VectorPtr &elements() const { return elements_; }

  const int32_t *rawOffsets() const { return offsets_->as<int32_t>(); }
  const int32_t *rawSizes() const { return sizes_->as<int32_t>(); }

  int32_t offsetAt(vector_size_t index) const { return rawOffsets()[index]; }
  int32_t sizeAt(vector_size_t index) const { return rawSizes()[index]; }

  std::string toString(vector_size_t index) const override {
    if (isNullAt(index)) {
      return std::string(kNullValueString);
    }

    const auto offset = offsetAt(index);
    const auto size = sizeAt(index);
    return stringifyTruncatedElementList(
        size, [this, offset](std::stringstream &out, size_t i) {
          out << elements_->toString(offset + i);
        });
  }

  int32_t compare(const BaseVector *other, vector_size_t index,
                  vector_size_t otherIndex) const override {
    auto *otherArray = static_cast<const ArrayVector *>(other);
    const auto size = sizeAt(index);
    const auto otherSize = otherArray->sizeAt(otherIndex);
    const auto minSize = std::min(size, otherSize);
    for (int32_t i = 0; i < minSize; ++i) {
      int32_t cmp =
          elements_->compare(otherArray->elements_.get(), offsetAt(index) + i,
                             otherArray->offsetAt(otherIndex) + i);
      if (cmp != 0)
        return cmp;
    }
    if (size < otherSize)
      return -1;
    if (size > otherSize)
      return 1;
    return 0;
  }

  void copy(const BaseVector * /*source*/, vector_size_t /*sourceIndex*/,
            vector_size_t /*targetIndex*/) override {
    VELOX_NYI("ArrayVector copy not implemented in ForkVelox");
  }

private:
  BufferPtr offsets_;
  BufferPtr sizes_;
  VectorPtr elements_;
};

class RowVector : public BaseVector {
public:
  using BaseVector::toString;

  RowVector(memory::MemoryPool *pool, std::shared_ptr<const Type> type,
            BufferPtr nulls, vector_size_t length,
            std::vector<VectorPtr> children)
      : BaseVector(pool, type, VectorEncoding::Simple::ROW, std::move(nulls),
                   length),
        children_(std::move(children)) {}

  /// Returns string representation of a single row.
  /// Format: {col1_value, col2_value, ...}
  std::string toString(vector_size_t index) const override {
    if (isNullAt(index)) {
      return std::string(kNullValueString);
    }

    // Use stringifyTruncatedElementList with limit = children size
    // to show all columns (no truncation for single row)
    return stringifyTruncatedElementList(
        children_.size(),
        [this, index](std::stringstream &out, size_t i) {
          out << (children_[i] ? children_[i]->toString(index) : "<not set>");
        },
        children_.size()); // No truncation for single row
  }

  /// Returns string representation of a range of rows [from, to).
  /// Format: Shows both schema and data.
  /// Example output:
  ///   ROW(a BIGINT, b BIGINT, dow VARCHAR): 4 elements
  ///   0: {0, 0, monday}
  ///   1: {1, 5, tuesday}
  ///   ...
  std::string toString(vector_size_t from, vector_size_t to,
                       const char *delimiter = "\n",
                       bool includeRowNumbers = true) const {
    const auto start = std::max<vector_size_t>(0, std::min(from, length_));
    const auto end = std::max<vector_size_t>(0, std::min(to, length_));

    std::stringstream out;

    // First line: Schema summary
    out << type_->toString() << ": " << (end - start) << " elements";

    // Subsequent lines: data rows
    for (auto i = start; i < end; ++i) {
      out << delimiter;
      if (includeRowNumbers) {
        out << i << ": ";
      }
      out << toString(i);
    }
    return out.str();
  }

  const std::vector<VectorPtr> &children() const { return children_; }
  const VectorPtr &childAt(int32_t index) const { return children_[index]; }

  vector_size_t childrenSize() const { return children_.size(); }

  int32_t compare(const BaseVector *other, vector_size_t index,
                  vector_size_t otherIndex) const override {
    // Compare child by child
    auto *otherRow = static_cast<const RowVector *>(other);
    for (size_t i = 0; i < children_.size(); ++i) {
      int cmp = children_[i]->compare(otherRow->children_[i].get(), index,
                                      otherIndex);
      if (cmp != 0)
        return cmp;
    }
    return 0;
  }

  void copy(const BaseVector *source, vector_size_t sourceIndex,
            vector_size_t targetIndex) override {
    auto *srcRow = static_cast<const RowVector *>(source);
    for (size_t i = 0; i < children_.size(); ++i) {
      children_[i]->copy(srcRow->children_[i].get(), sourceIndex, targetIndex);
    }
  }

private:
  std::vector<VectorPtr> children_;
};

} // namespace facebook::velox
