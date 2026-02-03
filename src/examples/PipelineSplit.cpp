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

// Demo: a multi-pipeline plan split by LocalPartition/LocalMerge.
int main(int argc, char **argv) {
  folly::init::Init init{&argc, &argv, false};

  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  functions::registerAllScalarFunctions();
  parse::registerTypeResolver();

  const auto rowType = ROW({"my_col"}, {BIGINT()});
  const vector_size_t batchSize = 6;
  const int numBatches = 4; // 24 rows total.

  std::vector<RowVectorPtr> batches;
  batches.reserve(numBatches);

  std::mt19937 rng(std::random_device{}());
  for (int batch = 0; batch < numBatches; ++batch) {
    auto buffer =
        AlignedBuffer::allocate(batchSize * sizeof(int64_t), pool.get());
    auto *rawValues = buffer->asMutable<int64_t>();
    std::iota(rawValues, rawValues + batchSize, batch * batchSize);
    std::shuffle(rawValues, rawValues + batchSize, rng);

    auto vector = std::make_shared<FlatVector<int64_t>>(
        pool.get(), BIGINT(), nullptr, batchSize, buffer);
    batches.push_back(
        std::make_shared<RowVector>(pool.get(), rowType, nullptr, batchSize,
                                    std::vector<VectorPtr>{vector}));
  }

  const std::string exchangeId = "pipeline_split_exchange";
  auto builder = PlanBuilder()
                     .values(batches)
                     .filter("my_col % 2 == 1")
                     .orderBy({"my_col"}, true)
                     .localPartition(exchangeId)
                     .localMerge(exchangeId)
                     .orderBy({"my_col"}, false);

  builder.printPlanTree("PipelineSplit Plan");
  auto plan = builder.planNode();

  auto task =
      Task::create("pipeline_split_task", plan, core::QueryCtx::create(),
                   Task::ExecutionMode::kParallel);
  task->setMaxDrivers(3);

  auto results = task->run();
  std::cout << "Pipeline-split task produced " << results.size() << " batches."
            << std::endl;

  std::vector<std::string> actualRows;
  for (const auto &batch : results) {
    if (!batch) {
      continue;
    }
    for (vector_size_t i = 0; i < batch->size(); ++i) {
      auto row = batch->toString(i);
      actualRows.push_back(row);
      std::cout << row << std::endl;
    }
  }

  std::vector<std::string> expectedRows;
  expectedRows.reserve(12);
  for (int64_t value = 1; value < batchSize * numBatches; value += 2) {
    expectedRows.push_back("{" + std::to_string(value) + "}");
  }
  VELOX_CHECK_EQ(actualRows.size(), expectedRows.size());
  VELOX_CHECK_EQ(actualRows, expectedRows);

  return 0;
}
