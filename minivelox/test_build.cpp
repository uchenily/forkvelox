#include "common/memory/Memory.h"
#include "type/Type.h"
#include "buffer/Buffer.h"
#include "vector/SimpleVector.h"
#include "vector/RowVector.h"
#include "exec/Expr.h"
#include <iostream>

using namespace facebook::velox;

int main() {
    auto pool = memory::MemoryManager::getInstance().addRootPool();
    auto vec = makeFlatVector<int32_t>({1, 2, 3}, pool);
    std::cout << vec->toString() << std::endl;
    return 0;
}
