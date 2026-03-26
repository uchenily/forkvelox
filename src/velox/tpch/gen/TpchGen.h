#pragma once
#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

#include <string>
#include <vector>

namespace facebook::velox::tpch {

enum Table { TBL_NATION, TBL_REGION, TBL_LINEITEM };

RowTypePtr getTableSchema(Table table);

RowVectorPtr readTable(
    memory::MemoryPool* pool,
    Table table,
    const std::vector<std::string>& columns,
    int scaleFactor);

RowVectorPtr readTableSplit(
    memory::MemoryPool* pool,
    Table table,
    const std::vector<std::string>& columns,
    int scaleFactor,
    int part,
    int totalParts);

}
