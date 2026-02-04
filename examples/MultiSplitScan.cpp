#include <folly/init/Init.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/RowVectorFile.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::test;

int main(int argc, char **argv) {
  bool onlyRead = true;
  if (argc >= 2 && std::string(argv[1]) == "write")
    onlyRead = false;
  std::cout << (onlyRead ? "read" : "write") << '\n';
  folly::init::Init init{&argc, &argv, false};

  memory::initializeMemoryManager(memory::MemoryManager::Options{});

  functions::registerAllScalarFunctions();
  parse::registerTypeResolver();

  const std::string file1 = "./data/multi_split_1.rv";
  const std::string file2 = "./data/multi_split_2.rv";

  VectorTestBase builder;
  const std::vector<std::string> columnNames = {"id", "count", "name"};
  auto batch1 = builder.makeRowVector(columnNames, {builder.makeFlatVector<int64_t>({1, 2, 3}),
                                                    builder.makeFlatVector<int32_t>({10, 20, 30}),
                                                    builder.makeFlatVector<std::string>({"alpha", "beta", "gamma"})});
  auto batch2 = builder.makeRowVector(columnNames, {builder.makeFlatVector<int64_t>({4, 5, 6}),
                                                    builder.makeFlatVector<int32_t>({40, 50, 60}),
                                                    builder.makeFlatVector<std::string>({"delta", "epsilon", "zeta"})});
  if (onlyRead) {
    batch1 = dwio::common::RowVectorFile::read(builder.pool(), file1);
    batch2 = dwio::common::RowVectorFile::read(builder.pool(), file2);
  } else {
    dwio::common::RowVectorFile::write(*batch1, file1);
    dwio::common::RowVectorFile::write(*batch2, file2);
  }

  core::PlanNodeId scanId;
  auto plan = PlanBuilder().tableScan(asRowType(batch1->type()), file1).capturePlanNodeId(scanId).planNode();

  auto task = Task::create("multi_split_scan", plan, core::QueryCtx::create(), Task::ExecutionMode::kParallel);
  task->setMaxDrivers(2);
  task->addSplit(scanId, Split(file1));
  task->addSplit(scanId, Split(file2));
  task->noMoreSplits(scanId);

  auto results = task->run();
  std::cout << "Multi-split task produced " << results.size() << " batches." << std::endl;
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

  std::vector<std::string> expectedRows = {"{1, 10, alpha}", "{2, 20, beta}",    "{3, 30, gamma}",
                                           "{4, 40, delta}", "{5, 50, epsilon}", "{6, 60, zeta}"};
  std::sort(actualRows.begin(), actualRows.end());
  std::sort(expectedRows.begin(), expectedRows.end());
  VELOX_CHECK_EQ(actualRows.size(), expectedRows.size());
  VELOX_CHECK_EQ(actualRows, expectedRows);

  return 0;
}
