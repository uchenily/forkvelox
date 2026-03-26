#include <folly/init/Init.h>

#include <algorithm>
#include <iostream>
#include <numeric>
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

int main(int argc, char** argv) {
  folly::init::Init init{&argc, &argv, false};

  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  functions::registerAllScalarFunctions();
  parse::registerTypeResolver();

  const auto rowType = ROW({"my_col"}, {BIGINT()});
  std::vector<RowVectorPtr> batches;
  for (int batch = 0; batch < 3; ++batch) {
    auto buffer = AlignedBuffer::allocate(4 * sizeof(int64_t), pool.get());
    auto* rawValues = buffer->asMutable<int64_t>();
    std::iota(rawValues, rawValues + 4, batch * 4);
    auto vector = std::make_shared<FlatVector<int64_t>>(pool.get(), BIGINT(), nullptr, 4, buffer);
    batches.push_back(
        std::make_shared<RowVector>(pool.get(), rowType, nullptr, 4, std::vector<VectorPtr>{vector}));
  }

  auto plan = PlanBuilder().values(batches).filter("my_col % 2 == 0").planNode();
  auto task = Task::create("serial_next_task", plan, core::QueryCtx::create());

  std::vector<std::string> rows;
  while (auto batch = task->next()) {
    for (vector_size_t i = 0; i < batch->size(); ++i) {
      rows.push_back(batch->toString(i));
      std::cout << batch->toString(i) << std::endl;
    }
  }

  std::vector<std::string> expected = {"{0}", "{2}", "{4}", "{6}", "{8}", "{10}"};
  std::sort(rows.begin(), rows.end());
  std::sort(expected.begin(), expected.end());
  VELOX_CHECK_EQ(rows, expected);
  return 0;
}
