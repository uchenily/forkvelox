#include "velox/vector/BaseVector.h"

#include <cstring>

#include "velox/common/base/Exceptions.h"
#include "velox/type/StringView.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox {

std::shared_ptr<BaseVector> BaseVector::create(const std::shared_ptr<const Type> &type, vector_size_t size,
                                               memory::MemoryPool *pool) {
  VELOX_CHECK(type != nullptr, "BaseVector::create requires non-null type");
  VELOX_CHECK(pool != nullptr, "BaseVector::create requires non-null memory pool");
  VELOX_CHECK_GE(size, 0, "BaseVector::create requires non-negative size");

  switch (type->kind()) {
  case TypeKind::BIGINT: {
    const auto bytes = size * static_cast<vector_size_t>(sizeof(int64_t));
    auto values = AlignedBuffer::allocate(bytes, pool);
    if (bytes > 0) {
      std::memset(values->as_mutable_uint8_t(), 0, bytes);
    }
    return std::make_shared<FlatVector<int64_t>>(pool, type, nullptr, size, values);
  }
  case TypeKind::INTEGER: {
    const auto bytes = size * static_cast<vector_size_t>(sizeof(int32_t));
    auto values = AlignedBuffer::allocate(bytes, pool);
    if (bytes > 0) {
      std::memset(values->as_mutable_uint8_t(), 0, bytes);
    }
    return std::make_shared<FlatVector<int32_t>>(pool, type, nullptr, size, values);
  }
  case TypeKind::VARCHAR: {
    const auto bytes = size * static_cast<vector_size_t>(sizeof(StringView));
    auto values = AlignedBuffer::allocate(bytes, pool);
    auto *rawValues = values->asMutable<StringView>();
    for (vector_size_t i = 0; i < size; ++i) {
      rawValues[i] = StringView();
    }
    return std::make_shared<FlatVector<StringView>>(pool, type, nullptr, size, values, std::vector<BufferPtr>{});
  }
  case TypeKind::ROW: {
    auto rowType = asRowType(type);
    VELOX_CHECK(rowType != nullptr, "ROW kind type must be RowType");
    std::vector<VectorPtr> children;
    children.reserve(rowType->size());
    for (size_t i = 0; i < rowType->size(); ++i) {
      children.push_back(BaseVector::create(rowType->childAt(i), size, pool));
    }
    return std::make_shared<RowVector>(pool, type, nullptr, size, std::move(children));
  }
  case TypeKind::ARRAY: {
    auto arrayType = std::dynamic_pointer_cast<const ArrayType>(type);
    VELOX_CHECK(arrayType != nullptr, "ARRAY kind type must be ArrayType");
    const auto bytes = size * static_cast<vector_size_t>(sizeof(int32_t));
    auto offsets = AlignedBuffer::allocate(bytes, pool);
    auto sizes = AlignedBuffer::allocate(bytes, pool);
    if (bytes > 0) {
      std::memset(offsets->as_mutable_uint8_t(), 0, bytes);
      std::memset(sizes->as_mutable_uint8_t(), 0, bytes);
    }
    auto elements = BaseVector::create(arrayType->elementType(), 0, pool);
    return std::make_shared<ArrayVector>(pool, type, nullptr, size, offsets, sizes, elements);
  }
  case TypeKind::FUNCTION:
    return std::make_shared<FunctionVector>(pool, type, size);

  default:
    VELOX_FAIL("BaseVector::create does not support type {}", type->toString());
  }
}

} // namespace facebook::velox
