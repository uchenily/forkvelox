#include <algorithm>
#include <filesystem>
#include <iostream>
#include <numeric>
#include <random>

#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

int main(int argc, char** argv) {
  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  auto inputRowType = ROW({"my_col"}, {BIGINT()});
  const vector_size_t vectorSize = 10;

  auto buffer = AlignedBuffer::allocate(vectorSize * sizeof(int64_t), pool.get());
  auto* rawValues = buffer->asMutable<int64_t>();
  std::iota(rawValues, rawValues + vectorSize, 0);
  std::mt19937 rng(std::random_device{}());
  std::shuffle(rawValues, rawValues + vectorSize, rng);

  auto vector = std::make_shared<FlatVector<int64_t>>(
      pool.get(), BIGINT(), nullptr, vectorSize, buffer);
  auto rowVector = std::make_shared<RowVector>(
      pool.get(), inputRowType, nullptr, vectorSize, std::vector<VectorPtr>{vector});

  std::cout << "Input vector generated:" << std::endl;
  for (vector_size_t i = 0; i < rowVector->size(); ++i) {
    std::cout << rowVector->toString(i) << std::endl;
  }

  std::filesystem::path tempDir =
      std::filesystem::temp_directory_path() / "forkvelox_scan_and_sort";
  std::filesystem::create_directories(tempDir);
  auto filePath = (tempDir / "scan_and_sort.tsv").string();

  auto writerPlan =
      PlanBuilder().values({rowVector}).tableWrite(filePath).planNode();
  AssertQueryBuilder(writerPlan).copyResults(pool.get());

  auto readPlan = PlanBuilder()
                      .tableScan(inputRowType, filePath)
                      .orderBy({"my_col"}, false)
                      .planNode();
  auto sorted = AssertQueryBuilder(readPlan).copyResults(pool.get());

  if (sorted) {
    std::cout << "Vector available after processing (scan + sort):" << std::endl;
    for (vector_size_t i = 0; i < sorted->size(); ++i) {
      std::cout << sorted->toString(i) << std::endl;
    }
  }

  return 0;
}
