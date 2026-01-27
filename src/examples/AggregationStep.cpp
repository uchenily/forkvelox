#include <folly/init/Init.h>

#include <algorithm>
#include <iostream>
#include <numeric>
#include <random>

#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace {

RowVectorPtr makeBatch(memory::MemoryPool* pool, const std::vector<int64_t>& values) {
  auto buffer = AlignedBuffer::allocate(values.size() * sizeof(int64_t), pool);
  std::memcpy(buffer->asMutable<uint8_t>(), values.data(), values.size() * sizeof(int64_t));
  auto vec = std::make_shared<FlatVector<int64_t>>(
      pool, BIGINT(), nullptr, values.size(), buffer);
  return std::make_shared<RowVector>(
      pool,
      ROW({"my_col"}, {BIGINT()}),
      nullptr,
      values.size(),
      std::vector<VectorPtr>{vec});
}

RowVectorPtr makeKeyValueBatch(
    memory::MemoryPool* pool,
    const std::vector<int64_t>& keys,
    const std::vector<int64_t>& values) {
  VELOX_CHECK_EQ(keys.size(), values.size());
  auto keyBuffer = AlignedBuffer::allocate(keys.size() * sizeof(int64_t), pool);
  auto valueBuffer = AlignedBuffer::allocate(values.size() * sizeof(int64_t), pool);
  std::memcpy(keyBuffer->asMutable<uint8_t>(), keys.data(), keys.size() * sizeof(int64_t));
  std::memcpy(valueBuffer->asMutable<uint8_t>(), values.data(), values.size() * sizeof(int64_t));
  auto keyVec = std::make_shared<FlatVector<int64_t>>(
      pool, BIGINT(), nullptr, keys.size(), keyBuffer);
  auto valueVec = std::make_shared<FlatVector<int64_t>>(
      pool, BIGINT(), nullptr, values.size(), valueBuffer);
  return std::make_shared<RowVector>(
      pool,
      ROW({"k", "v"}, {BIGINT(), BIGINT()}),
      nullptr,
      keys.size(),
      std::vector<VectorPtr>{keyVec, valueVec});
}

std::vector<std::string> collectRows(const std::vector<RowVectorPtr>& results) {
  std::vector<std::string> rows;
  for (const auto& batch : results) {
    if (!batch) {
      continue;
    }
    for (vector_size_t i = 0; i < batch->size(); ++i) {
      rows.push_back(batch->toString(i));
    }
  }
  return rows;
}

void runTaskWithExpectedRows(
    const std::string& name,
    const core::PlanNodePtr& plan,
    size_t maxDrivers,
    const std::vector<std::string>& expectedRows,
    bool orderMatters) {
  auto task = Task::create(
      name,
      plan,
      core::QueryCtx::create(),
      Task::ExecutionMode::kParallel);
  task->setMaxDrivers(maxDrivers);
  auto results = task->run();
  std::cout << name << " produced " << results.size() << " batches." << std::endl;
  for (const auto& batch : results) {
    if (!batch) {
      continue;
    }
    for (vector_size_t i = 0; i < batch->size(); ++i) {
      std::cout << batch->toString(i) << std::endl;
    }
  }
  auto actualRows = collectRows(results);
  if (!orderMatters) {
    auto expectedSorted = expectedRows;
    std::sort(actualRows.begin(), actualRows.end());
    std::sort(expectedSorted.begin(), expectedSorted.end());
    VELOX_CHECK_EQ(actualRows.size(), expectedSorted.size());
    VELOX_CHECK_EQ(actualRows, expectedSorted);
  } else {
    VELOX_CHECK_EQ(actualRows.size(), expectedRows.size());
    VELOX_CHECK_EQ(actualRows, expectedRows);
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::init::Init init{&argc, &argv, false};

  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  functions::registerAllScalarFunctions();
  parse::registerTypeResolver();

  auto batch = makeBatch(pool.get(), {1, 2, 3, 4, 5, 6});
  std::vector<RowVectorPtr> batches{batch};

  auto singlePlan = PlanBuilder()
                        .values(batches)
                        .singleAggregation({}, {"sum(my_col) AS sum_col", "count(1) AS cnt", "avg(my_col) AS avg_col"})
                        .planNode();

  std::cout << "Running single-step aggregation." << std::endl;
  runTaskWithExpectedRows("agg_single", singlePlan, 3, {"{21, 6, 3}"}, false);

  const std::string exchangeId = "agg_exchange";
  auto partialFinalPlan = PlanBuilder()
                              .values(batches)
                              .partialAggregation({}, {"sum(my_col) AS sum_col", "count(1) AS cnt", "avg(my_col) AS avg_col"})
                              .localPartition(exchangeId)
                              .localMerge(exchangeId)
                              .finalAggregation({}, {"sum(my_col) AS sum_col", "count(1) AS cnt", "avg(my_col) AS avg_col"})
                              .planNode();

  std::cout << "Running partial/final aggregation." << std::endl;
  runTaskWithExpectedRows("agg_partial_final", partialFinalPlan, 3, {"{21, 6, 3}"}, false);

  const std::string exchangeId1 = "agg_exchange_1";
  const std::string exchangeId2 = "agg_exchange_2";
  auto intermediatePlan = PlanBuilder()
                              .values(batches)
                              .partialAggregation({}, {"sum(my_col) AS sum_col", "count(1) AS cnt", "avg(my_col) AS avg_col"})
                              .localPartition(exchangeId1)
                              .localMerge(exchangeId1)
                              .intermediateAggregation({}, {"sum(my_col) AS sum_col", "count(1) AS cnt", "avg(my_col) AS avg_col"})
                              .localPartition(exchangeId2)
                              .localMerge(exchangeId2)
                              .finalAggregation({}, {"sum(my_col) AS sum_col", "count(1) AS cnt", "avg(my_col) AS avg_col"})
                              .planNode();

  std::cout << "Running partial/intermediate/final aggregation." << std::endl;
  runTaskWithExpectedRows("agg_intermediate", intermediatePlan, 3, {"{21, 6, 3}"}, false);

  auto groupedBatch = makeKeyValueBatch(
      pool.get(), {1, 1, 2, 2, 3, 3}, {10, 20, 30, 40, 50, 60});
  std::vector<RowVectorPtr> groupedBatches{groupedBatch};
  auto groupedPlan = PlanBuilder()
                         .values(groupedBatches)
                         .singleAggregation({"k"}, {"sum(v) AS sum_v", "count(1) AS cnt"})
                         .planNode();

  std::cout << "Running group-by aggregation." << std::endl;
  runTaskWithExpectedRows(
      "agg_group_by",
      groupedPlan,
      3,
      {"{1, 30, 2}", "{2, 70, 2}", "{3, 110, 2}"},
      false);

  return 0;
}
