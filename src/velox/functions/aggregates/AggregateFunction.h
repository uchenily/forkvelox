#pragma once

#include "velox/vector/ComplexVector.h"
#include <memory>
#include <string>

namespace facebook::velox::aggregate {

struct AggregateAccumulator {
  int64_t sum{0};
  int64_t count{0};
};

class AggregateFunction {
public:
  virtual ~AggregateFunction() = default;

  virtual void addRaw(const facebook::velox::RowVectorPtr &input,
                      facebook::velox::vector_size_t row, int argIndex,
                      AggregateAccumulator &acc) const = 0;

  virtual void addIntermediate(const facebook::velox::RowVectorPtr &input,
                               facebook::velox::vector_size_t row,
                               int valueIndex, int countIndex,
                               AggregateAccumulator &acc) const = 0;

  virtual int64_t finalize(const AggregateAccumulator &acc) const = 0;

  virtual bool usesSumAndCount() const { return false; }
};

void registerAggregateFunction(const std::string &name,
                               std::shared_ptr<AggregateFunction> func);

std::shared_ptr<AggregateFunction>
getAggregateFunction(const std::string &name);

} // namespace facebook::velox::aggregate
