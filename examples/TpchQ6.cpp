#include <folly/init/Init.h>

#include <iomanip>
#include <iostream>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

int main(int argc, char** argv) {
  folly::init::Init init{&argc, &argv, false};

  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  functions::registerAllScalarFunctions();
  aggregate::registerAllAggregateFunctions();
  parse::registerTypeResolver();

  auto plan = PlanBuilder()
                  .tpchTableScan(tpch::Table::TBL_LINEITEM, {"l_shipdate", "l_discount", "l_quantity", "l_revenue"}, 1)
                  .filter("l_shipdate >= 19940101")
                  .filter("l_shipdate < 19950101")
                  .filter("l_discount >= 5")
                  .filter("l_discount <= 7")
                  .filter("l_quantity < 24")
                  .singleAggregation({}, {"sum(l_revenue) AS revenue_x10000"})
                  .planNode();

  auto result = AssertQueryBuilder(plan).copyResults(pool.get());
  VELOX_CHECK(result != nullptr, "TPC-H Q6 returned no rows");
  VELOX_CHECK_EQ(result->size(), 1);

  auto revenue = std::dynamic_pointer_cast<SimpleVector<int64_t>>(result->childAt(0));
  VELOX_CHECK(revenue != nullptr, "TPC-H Q6 expected BIGINT revenue result");

  constexpr int64_t kExpectedRevenueX10000 = 144924440;
  VELOX_CHECK_EQ(revenue->valueAt(0), kExpectedRevenueX10000);

  const auto whole = revenue->valueAt(0) / 10000;
  const auto fraction = revenue->valueAt(0) % 10000;
  std::cout << "Q6 revenue_x10000: " << revenue->valueAt(0) << std::endl;
  std::cout << "Q6 revenue: " << whole << '.' << std::setw(4) << std::setfill('0') << fraction << std::endl;
  return 0;
}
