#include "velox/buffer/Buffer.h"
#include "velox/expression/FunctionRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/FunctionVector.h"
#include <cstring>

namespace facebook::velox::functions {

using namespace facebook::velox::exec;

namespace {

VectorPtr sliceElements(const VectorPtr &elements, vector_size_t offset, vector_size_t size, memory::MemoryPool *pool) {
  auto simple = std::dynamic_pointer_cast<SimpleVector<int64_t>>(elements);
  VELOX_CHECK_NOT_NULL(simple.get(), "transform supports ARRAY<BIGINT> in ForkVelox");

  auto buffer = AlignedBuffer::allocate(size * sizeof(int64_t), pool);
  auto flat = std::make_shared<FlatVector<int64_t>>(pool, BIGINT(), nullptr, size, buffer);
  auto *raw = flat->mutableRawValues();
  for (vector_size_t i = 0; i < size; ++i) {
    raw[i] = simple->valueAt(offset + i);
  }
  return flat;
}

class TransformFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args, const TypePtr &outputType, EvalCtx &context,
             VectorPtr &result) const override {
    VELOX_CHECK_EQ(args.size(), 2, "transform expects 2 arguments");

    auto arrayVector = std::dynamic_pointer_cast<ArrayVector>(args[0]);
    VELOX_CHECK_NOT_NULL(arrayVector.get(), "transform expects ARRAY input");

    auto functionVector = std::dynamic_pointer_cast<FunctionVector>(args[1]);
    VELOX_CHECK_NOT_NULL(functionVector.get(), "transform expects lambda input");

    const auto rowCount = rows.size();
    std::vector<int32_t> offsets(rowCount, 0);
    std::vector<int32_t> sizes(rowCount, 0);
    std::vector<int64_t> resultValues;

    auto iterator = functionVector->iterator(&rows);
    while (true) {
      auto entry = iterator.next();
      if (!entry) {
        break;
      }
      entry.rows->applyToSelected([&](vector_size_t row) {
        const auto size = arrayVector->sizeAt(row);
        offsets[row] = static_cast<int32_t>(resultValues.size());
        sizes[row] = size;
        if (size == 0) {
          return;
        }

        auto elementVector = sliceElements(arrayVector->elements(), arrayVector->offsetAt(row), size, context.pool());

        VectorPtr lambdaResult;
        SelectivityVector elementRows(size, true);
        entry.callable->apply(row, elementRows, context, {elementVector}, lambdaResult);

        auto outValues = std::dynamic_pointer_cast<SimpleVector<int64_t>>(lambdaResult);
        VELOX_CHECK_NOT_NULL(outValues.get(), "transform expects lambda to return BIGINT");

        for (vector_size_t i = 0; i < size; ++i) {
          resultValues.push_back(outValues->valueAt(i));
        }
      });
    }

    auto offsetsBuffer = AlignedBuffer::allocate(rowCount * sizeof(int32_t), context.pool());
    auto sizesBuffer = AlignedBuffer::allocate(rowCount * sizeof(int32_t), context.pool());
    std::memcpy(offsetsBuffer->asMutable<uint8_t>(), offsets.data(), offsetsBuffer->size());
    std::memcpy(sizesBuffer->asMutable<uint8_t>(), sizes.data(), sizesBuffer->size());

    auto valuesBuffer = AlignedBuffer::allocate(resultValues.size() * sizeof(int64_t), context.pool());
    std::memcpy(valuesBuffer->asMutable<uint8_t>(), resultValues.data(), valuesBuffer->size());

    auto values = std::make_shared<FlatVector<int64_t>>(context.pool(), BIGINT(), nullptr,
                                                        static_cast<vector_size_t>(resultValues.size()), valuesBuffer);

    result = std::make_shared<ArrayVector>(context.pool(), outputType, nullptr, rowCount, offsetsBuffer, sizesBuffer,
                                           values);
  }
};

class FilterFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args, const TypePtr &outputType, EvalCtx &context,
             VectorPtr &result) const override {
    VELOX_CHECK_EQ(args.size(), 2, "filter expects 2 arguments");

    auto arrayVector = std::dynamic_pointer_cast<ArrayVector>(args[0]);
    VELOX_CHECK_NOT_NULL(arrayVector.get(), "filter expects ARRAY input");

    auto functionVector = std::dynamic_pointer_cast<FunctionVector>(args[1]);
    VELOX_CHECK_NOT_NULL(functionVector.get(), "filter expects lambda input");

    const auto rowCount = rows.size();
    std::vector<int32_t> offsets(rowCount, 0);
    std::vector<int32_t> sizes(rowCount, 0);
    std::vector<int64_t> resultValues;

    auto iterator = functionVector->iterator(&rows);
    while (true) {
      auto entry = iterator.next();
      if (!entry) {
        break;
      }
      entry.rows->applyToSelected([&](vector_size_t row) {
        const auto size = arrayVector->sizeAt(row);
        offsets[row] = static_cast<int32_t>(resultValues.size());
        sizes[row] = 0;
        if (size == 0) {
          return;
        }

        auto elementVector = sliceElements(arrayVector->elements(), arrayVector->offsetAt(row), size, context.pool());

        VectorPtr lambdaResult;
        SelectivityVector elementRows(size, true);
        entry.callable->apply(row, elementRows, context, {elementVector}, lambdaResult);

        auto predicate = std::dynamic_pointer_cast<SimpleVector<int32_t>>(lambdaResult);
        VELOX_CHECK_NOT_NULL(predicate.get(), "filter expects lambda to return BOOLEAN");

        auto elementValues = std::dynamic_pointer_cast<SimpleVector<int64_t>>(elementVector);
        VELOX_CHECK_NOT_NULL(elementValues.get(), "filter supports ARRAY<BIGINT> in ForkVelox");

        for (vector_size_t i = 0; i < size; ++i) {
          if (predicate->isNullAt(i)) {
            continue;
          }
          if (predicate->valueAt(i)) {
            resultValues.push_back(elementValues->valueAt(i));
            sizes[row] += 1;
          }
        }
      });
    }

    auto offsetsBuffer = AlignedBuffer::allocate(rowCount * sizeof(int32_t), context.pool());
    auto sizesBuffer = AlignedBuffer::allocate(rowCount * sizeof(int32_t), context.pool());
    std::memcpy(offsetsBuffer->asMutable<uint8_t>(), offsets.data(), offsetsBuffer->size());
    std::memcpy(sizesBuffer->asMutable<uint8_t>(), sizes.data(), sizesBuffer->size());

    auto valuesBuffer = AlignedBuffer::allocate(resultValues.size() * sizeof(int64_t), context.pool());
    std::memcpy(valuesBuffer->asMutable<uint8_t>(), resultValues.data(), valuesBuffer->size());

    auto values = std::make_shared<FlatVector<int64_t>>(context.pool(), BIGINT(), nullptr,
                                                        static_cast<vector_size_t>(resultValues.size()), valuesBuffer);

    result = std::make_shared<ArrayVector>(context.pool(), outputType, nullptr, rowCount, offsetsBuffer, sizesBuffer,
                                           values);
  }
};

} // namespace

void registerLambdaFunctions() {
  exec::registerFunction("transform", std::make_shared<TransformFunction>());
  exec::registerFunction("filter", std::make_shared<FilterFunction>());
}

} // namespace facebook::velox::functions
