#include <folly/init/Init.h>

#include <algorithm>
#include <iostream>
#include <numeric>
#include <random>

#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace {

std::vector<RowVectorPtr> makeBatches(
    memory::MemoryPool* pool,
    const RowTypePtr& rowType,
    vector_size_t batchSize,
    int numBatches) {
  std::vector<RowVectorPtr> batches;
  batches.reserve(numBatches);
  std::mt19937 rng(std::random_device{}());
  for (int batch = 0; batch < numBatches; ++batch) {
    auto buffer =
        AlignedBuffer::allocate(batchSize * sizeof(int64_t), pool);
    auto* rawValues = buffer->asMutable<int64_t>();
    std::iota(rawValues, rawValues + batchSize, batch * batchSize);
    std::shuffle(rawValues, rawValues + batchSize, rng);

    auto vector = std::make_shared<FlatVector<int64_t>>(
        pool, BIGINT(), nullptr, batchSize, buffer);
    batches.push_back(std::make_shared<RowVector>(
        pool, rowType, nullptr, batchSize, std::vector<VectorPtr>{vector}));
  }
  return batches;
}

void runTask(
    const std::string& name,
    const core::PlanNodePtr& plan,
    size_t maxDrivers) {
  auto task = Task::create(
      name,
      plan,
      core::QueryCtx::create(),
      Task::ExecutionMode::kParallel);
  task->setMaxDrivers(maxDrivers);
  auto results = task->run();
  std::cout << name << " produced " << results.size() << " batches."
            << std::endl;
}

} // namespace

int main(int argc, char** argv) {
  folly::init::Init init{&argc, &argv, false};

  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  functions::prestosql::registerAllScalarFunctions();
  parse::registerTypeResolver();

  const auto rowType = ROW({"my_col"}, {BIGINT()});
  const vector_size_t batchSize = 6;
  const int numBatches = 4;

  auto batches = makeBatches(pool.get(), rowType, batchSize, numBatches);

  // Partial OrderBy only: Velox allows multiple drivers.
  auto partialPlan = PlanBuilder()
                         .values(batches)
                         .orderBy({"my_col"}, true)
                         .planNode();

  // Partial + Final OrderBy with LocalExchange between stages.
  const std::string exchangeId = "orderby_exchange";
  auto finalPlan = PlanBuilder()
                       .values(batches)
                       .orderBy({"my_col"}, true)
                       .localPartition(exchangeId)
                       .localMerge(exchangeId)
                       .orderBy({"my_col"}, false)
                       .planNode();

  std::cout << "Running partial OrderBy (expect multiple drivers)." << std::endl;
  runTask("partial_orderby_task", partialPlan, 3);

  std::cout << "Running final OrderBy (expect single driver)." << std::endl;
  runTask("final_orderby_task", finalPlan, 3);

  return 0;
}
