#include "velox/common/dynamic_registry/DynamicLibraryLoader.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/ExecCtx.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/FunctionRegistry.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

using namespace facebook::velox;

int main() {
  memory::initializeMemoryManager({});

  auto queryCtx = core::QueryCtx::create();
  core::ExecCtx execCtx(queryCtx->pool(), queryCtx.get());

  if (exec::getVectorFunction("dynamic") != nullptr) {
    std::cerr << "dynamic() should not be registered before loading." << std::endl;
    return 1;
  }

  const std::string libraryPath = VELOX_DYNAMIC_EXAMPLE_LIB_PATH;
  loadDynamicLibrary(libraryPath);

  auto fn = exec::getVectorFunction("dynamic");
  if (!fn) {
    std::cerr << "dynamic() was not registered after loading." << std::endl;
    return 1;
  }

  const vector_size_t size = 4;
  SelectivityVector rows(size);
  std::vector<VectorPtr> args;
  exec::EvalCtx context(&execCtx, nullptr, nullptr);
  VectorPtr result;

  fn->apply(rows, args, BIGINT(), context, result);

  auto flat = std::dynamic_pointer_cast<FlatVector<int64_t>>(result);
  if (!flat) {
    std::cerr << "dynamic() did not return BIGINT FlatVector." << std::endl;
    return 1;
  }

  bool ok = true;
  rows.applyToSelected([&](vector_size_t i) {
    if (flat->valueAt(i) != 123) {
      ok = false;
    }
  });

  std::cout << "dynamic() result: \n" << result->toString() << std::endl;
  if (!ok) {
    std::cerr << "dynamic() returned unexpected values." << std::endl;
    return 1;
  }

  return 0;
}
