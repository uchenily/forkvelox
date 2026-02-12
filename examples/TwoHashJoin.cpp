#include <folly/init/Init.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <vector>

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

std::shared_ptr<FlatVector<int64_t>> makeInt64Vector(memory::MemoryPool *pool, const std::vector<int64_t> &data) {
  auto buffer = AlignedBuffer::allocate(data.size() * sizeof(int64_t), pool);
  std::memcpy(buffer->asMutable<uint8_t>(), data.data(), data.size() * sizeof(int64_t));
  return std::make_shared<FlatVector<int64_t>>(pool, BIGINT(), nullptr, data.size(), buffer);
}

std::shared_ptr<FlatVector<StringView>> makeStringVector(memory::MemoryPool *pool,
                                                         const std::vector<std::string> &data) {
  const size_t size = data.size();
  auto values = AlignedBuffer::allocate(size * sizeof(StringView), pool);
  auto *rawValues = values->asMutable<StringView>();
  size_t totalLen = 0;
  for (const auto &s : data) {
    totalLen += s.size();
  }
  auto dataBuffer = AlignedBuffer::allocate(totalLen, pool);
  char *bufPtr = dataBuffer->asMutable<char>();
  size_t offset = 0;
  for (size_t i = 0; i < size; ++i) {
    std::memcpy(bufPtr + offset, data[i].data(), data[i].size());
    rawValues[i] = StringView(bufPtr + offset, data[i].size());
    offset += data[i].size();
  }
  auto vec = std::make_shared<FlatVector<StringView>>(pool, VARCHAR(), nullptr, size, values);
  vec->addStringBuffer(dataBuffer);
  return vec;
}

RowVectorPtr makeRowVector(memory::MemoryPool *pool, const std::vector<std::string> &names,
                           const std::vector<TypePtr> &types, const std::vector<VectorPtr> &children) {
  return std::make_shared<RowVector>(pool, ROW(names, types), nullptr, children[0]->size(), children);
}

} // namespace

int main(int argc, char **argv) {
  folly::init::Init init{&argc, &argv, false};

  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  functions::registerAllScalarFunctions();
  parse::registerTypeResolver();

  auto probeBatch = makeRowVector(pool.get(), {"key"}, {BIGINT()}, {makeInt64Vector(pool.get(), {0, 1, 2, 3, 4})});

  auto buildBatch1 = makeRowVector(
      pool.get(), {"key", "name"}, {BIGINT(), VARCHAR()},
      {makeInt64Vector(pool.get(), {0, 1, 2, 3}), makeStringVector(pool.get(), {"R0", "R1", "R2", "R3"})});

  auto buildBatch2 = makeRowVector(
      pool.get(), {"key", "name"}, {BIGINT(), VARCHAR()},
      {makeInt64Vector(pool.get(), {1, 2, 3, 4}), makeStringVector(pool.get(), {"C1", "C2", "C3", "C4"})});

  auto idGen = std::make_shared<core::PlanNodeIdGenerator>();
  auto buildPlan1 = PlanBuilder(idGen).values({buildBatch1}).planNode();
  auto buildPlan2 = PlanBuilder(idGen).values({buildBatch2}).planNode();

  auto builder =
      PlanBuilder(idGen).values({probeBatch}).hashJoin({}, {}, buildPlan1, "", {}).hashJoin({}, {}, buildPlan2, "", {});

  builder.printPlanTree("TwoJoin Plan");
  auto plan = builder.planNode();
  std::cout << plan->toString(true, true) << '\n';

  auto queryCtx = core::QueryCtx::create();
  auto task = Task::create("two_hash_join_demo", plan, queryCtx, Task::ExecutionMode::kParallel);
  task->setMaxDrivers(2);

  auto results = task->run();
  std::cout << "Two-join task produced " << results.size() << " batches." << std::endl;
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

  std::vector<std::string> expectedRows = {"{1, C1}", "{2, C2}", "{3, C3}"};
  std::sort(actualRows.begin(), actualRows.end());
  std::sort(expectedRows.begin(), expectedRows.end());
  VELOX_CHECK_EQ(actualRows.size(), expectedRows.size());
  VELOX_CHECK_EQ(actualRows, expectedRows);

  return 0;
}
