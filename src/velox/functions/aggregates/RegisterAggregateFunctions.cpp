#include "velox/functions/aggregates/RegisterAggregateFunctions.h"

#include "velox/common/base/Exceptions.h"
#include "velox/functions/aggregates/AggregateFunction.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate {
namespace {

double readNumeric(const facebook::velox::VectorPtr& vec, facebook::velox::vector_size_t row) {
  if (vec->isNullAt(row)) {
    return 0;
  }
  switch (vec->type()->kind()) {
  case facebook::velox::TypeKind::BIGINT: {
    auto simple = std::dynamic_pointer_cast<facebook::velox::SimpleVector<int64_t>>(vec);
    VELOX_CHECK(simple, "Expected BIGINT vector.");
    return static_cast<double>(simple->valueAt(row));
  }
  case facebook::velox::TypeKind::INTEGER: {
    auto simple = std::dynamic_pointer_cast<facebook::velox::SimpleVector<int32_t>>(vec);
    VELOX_CHECK(simple, "Expected INTEGER vector.");
    return static_cast<double>(simple->valueAt(row));
  }
  case facebook::velox::TypeKind::DOUBLE: {
    auto simple = std::dynamic_pointer_cast<facebook::velox::SimpleVector<double>>(vec);
    VELOX_CHECK(simple, "Expected DOUBLE vector.");
    return simple->valueAt(row);
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
      acc.sum += readNumeric(vec, row);
    }
  }

  void addIntermediate(const facebook::velox::RowVectorPtr &input, facebook::velox::vector_size_t row, int valueIndex,
                       int /*countIndex*/, AggregateAccumulator &acc) const override {
    VELOX_CHECK_GE(valueIndex, 0);
    auto vec = input->childAt(valueIndex);
    if (!vec->isNullAt(row)) {
      acc.sum += readNumeric(vec, row);
    }
  }

  TypePtr resultType(const TypePtr& inputType) const override {
    return inputType && inputType->kind() == TypeKind::DOUBLE ? DOUBLE() : BIGINT();
  }

  Variant finalize(const AggregateAccumulator& acc, const TypePtr& resultType) const override {
    if (resultType->kind() == TypeKind::DOUBLE) {
      return Variant(acc.sum);
    }
    return Variant(static_cast<int64_t>(acc.sum));
  }
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
      acc.count += static_cast<int64_t>(readNumeric(vec, row));
    }
  }

  TypePtr resultType(const TypePtr& /*inputType*/) const override { return BIGINT(); }

  Variant finalize(const AggregateAccumulator& acc, const TypePtr& /*resultType*/) const override {
    return Variant(acc.count);
  }
};

class AvgAggregate : public AggregateFunction {
public:
  void addRaw(const facebook::velox::RowVectorPtr &input, facebook::velox::vector_size_t row, int argIndex,
              AggregateAccumulator &acc) const override {
    VELOX_CHECK_GE(argIndex, 0);
    auto vec = input->childAt(argIndex);
    if (!vec->isNullAt(row)) {
      acc.sum += readNumeric(vec, row);
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
      acc.sum += readNumeric(sumVec, row);
    }
    if (!countVec->isNullAt(row)) {
      acc.count += static_cast<int64_t>(readNumeric(countVec, row));
    }
  }

  TypePtr resultType(const TypePtr& /*inputType*/) const override { return DOUBLE(); }

  Variant finalize(const AggregateAccumulator& acc, const TypePtr& /*resultType*/) const override {
    return Variant(acc.count == 0 ? 0.0 : (acc.sum / static_cast<double>(acc.count)));
  }

  TypePtr partialSumType(const TypePtr& /*inputType*/) const override { return DOUBLE(); }

  bool usesSumAndCount() const override { return true; }
};

} // namespace

void registerAllAggregateFunctions() {
  registerAggregateFunction("sum", std::make_shared<SumAggregate>());
  registerAggregateFunction("count", std::make_shared<CountAggregate>());
  registerAggregateFunction("avg", std::make_shared<AvgAggregate>());
}

} // namespace facebook::velox::aggregate
