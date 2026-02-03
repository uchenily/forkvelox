#include "velox/functions/aggregates/RegisterAggregateFunctions.h"

#include "velox/common/base/Exceptions.h"
#include "velox/functions/aggregates/AggregateFunction.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate {
namespace {

int64_t readInt64(const facebook::velox::VectorPtr &vec, facebook::velox::vector_size_t row) {
  if (vec->isNullAt(row)) {
    return 0;
  }
  switch (vec->type()->kind()) {
  case facebook::velox::TypeKind::BIGINT: {
    auto simple = std::dynamic_pointer_cast<facebook::velox::SimpleVector<int64_t>>(vec);
    VELOX_CHECK(simple, "Expected BIGINT vector.");
    return simple->valueAt(row);
  }
  case facebook::velox::TypeKind::INTEGER: {
    auto simple = std::dynamic_pointer_cast<facebook::velox::SimpleVector<int32_t>>(vec);
    VELOX_CHECK(simple, "Expected INTEGER vector.");
    return static_cast<int64_t>(simple->valueAt(row));
  }
  default:
    VELOX_FAIL("Unsupported numeric type for aggregation input.");
  }
}

class SumAggregate : public AggregateFunction {
public:
  void addRaw(const facebook::velox::RowVectorPtr &input, facebook::velox::vector_size_t row, int argIndex,
              AggregateAccumulator &acc) const override {
    VELOX_CHECK_GE(argIndex, 0);
    auto vec = input->childAt(argIndex);
    if (!vec->isNullAt(row)) {
      acc.sum += readInt64(vec, row);
    }
  }

  void addIntermediate(const facebook::velox::RowVectorPtr &input, facebook::velox::vector_size_t row, int valueIndex,
                       int /*countIndex*/, AggregateAccumulator &acc) const override {
    VELOX_CHECK_GE(valueIndex, 0);
    auto vec = input->childAt(valueIndex);
    if (!vec->isNullAt(row)) {
      acc.sum += readInt64(vec, row);
    }
  }

  int64_t finalize(const AggregateAccumulator &acc) const override { return acc.sum; }
};

class CountAggregate : public AggregateFunction {
public:
  void addRaw(const facebook::velox::RowVectorPtr &input, facebook::velox::vector_size_t row, int argIndex,
              AggregateAccumulator &acc) const override {
    if (argIndex < 0) {
      acc.count++;
      return;
    }
    auto vec = input->childAt(argIndex);
    if (!vec->isNullAt(row)) {
      acc.count++;
    }
  }

  void addIntermediate(const facebook::velox::RowVectorPtr &input, facebook::velox::vector_size_t row, int valueIndex,
                       int /*countIndex*/, AggregateAccumulator &acc) const override {
    VELOX_CHECK_GE(valueIndex, 0);
    auto vec = input->childAt(valueIndex);
    if (!vec->isNullAt(row)) {
      acc.count += readInt64(vec, row);
    }
  }

  int64_t finalize(const AggregateAccumulator &acc) const override { return acc.count; }
};

class AvgAggregate : public AggregateFunction {
public:
  void addRaw(const facebook::velox::RowVectorPtr &input, facebook::velox::vector_size_t row, int argIndex,
              AggregateAccumulator &acc) const override {
    VELOX_CHECK_GE(argIndex, 0);
    auto vec = input->childAt(argIndex);
    if (!vec->isNullAt(row)) {
      acc.sum += readInt64(vec, row);
      acc.count++;
    }
  }

  void addIntermediate(const facebook::velox::RowVectorPtr &input, facebook::velox::vector_size_t row, int valueIndex,
                       int countIndex, AggregateAccumulator &acc) const override {
    VELOX_CHECK_GE(valueIndex, 0);
    VELOX_CHECK_GE(countIndex, 0);
    auto sumVec = input->childAt(valueIndex);
    auto countVec = input->childAt(countIndex);
    if (!sumVec->isNullAt(row)) {
      acc.sum += readInt64(sumVec, row);
    }
    if (!countVec->isNullAt(row)) {
      acc.count += readInt64(countVec, row);
    }
  }

  int64_t finalize(const AggregateAccumulator &acc) const override {
    return acc.count == 0 ? 0 : (acc.sum / acc.count);
  }

  bool usesSumAndCount() const override { return true; }
};

} // namespace

void registerAllAggregateFunctions() {
  registerAggregateFunction("sum", std::make_shared<SumAggregate>());
  registerAggregateFunction("count", std::make_shared<CountAggregate>());
  registerAggregateFunction("avg", std::make_shared<AvgAggregate>());
}

} // namespace facebook::velox::aggregate
