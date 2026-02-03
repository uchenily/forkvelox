#include <folly/init/Init.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <vector>

#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/RowVectorFile.h"
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

RowVectorPtr makeRowVector(memory::MemoryPool *pool,
                           const std::vector<int64_t> &values) {
  auto buffer = AlignedBuffer::allocate(values.size() * sizeof(int64_t), pool);
  std::memcpy(buffer->asMutable<uint8_t>(), values.data(),
              values.size() * sizeof(int64_t));
  auto vector = std::make_shared<FlatVector<int64_t>>(pool, BIGINT(), nullptr,
                                                      values.size(), buffer);
  return std::make_shared<RowVector>(pool, ROW({"my_col"}, {BIGINT()}), nullptr,
                                     values.size(),
                                     std::vector<VectorPtr>{vector});
}

} // namespace

int main(int argc, char **argv) {
  folly::init::Init init{&argc, &argv, false};

  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  functions::registerAllScalarFunctions();
  parse::registerTypeResolver();

  const std::string file1 = "./data/multi_split_1.rv";
  const std::string file2 = "./data/multi_split_2.rv";

  // auto batch1 = makeRowVector(pool.get(), {1, 2, 3});
  // auto batch2 = makeRowVector(pool.get(), {4, 5, 6});
  //
  // dwio::common::RowVectorFile::write(*batch1, file1);
  // dwio::common::RowVectorFile::write(*batch2, file2);
  auto batch1 = dwio::common::RowVectorFile::read(pool.get(), file1);
  auto batch2 = dwio::common::RowVectorFile::read(pool.get(), file2);

  core::PlanNodeId scanId;
  auto plan = PlanBuilder()
                  .tableScan(asRowType(batch1->type()), file1)
                  .capturePlanNodeId(scanId)
                  .planNode();

  auto task = Task::create("multi_split_scan", plan, core::QueryCtx::create(),
                           Task::ExecutionMode::kParallel);
  task->setMaxDrivers(2);
  task->addSplit(scanId, Split(file1));
  task->addSplit(scanId, Split(file2));
  task->noMoreSplits(scanId);

  auto results = task->run();
  std::cout << "Multi-split task produced " << results.size() << " batches."
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

  std::vector<std::string> expectedRows = {"{1}", "{2}", "{3}",
                                           "{4}", "{5}", "{6}"};
  std::sort(actualRows.begin(), actualRows.end());
  std::sort(expectedRows.begin(), expectedRows.end());
  VELOX_CHECK_EQ(actualRows.size(), expectedRows.size());
  VELOX_CHECK_EQ(actualRows, expectedRows);

  return 0;
}
