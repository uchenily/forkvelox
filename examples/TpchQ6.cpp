#include <folly/init/Init.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string_view>
#include <thread>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/exec/Task.h"
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

  std::string answerOutputPath;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg(argv[i]);
    if (arg == "--answer-output") {
      VELOX_CHECK(i + 1 < argc, "--answer-output requires a path");
      answerOutputPath = argv[++i];
      continue;
    }
  }

  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  functions::registerAllScalarFunctions();
  aggregate::registerAllAggregateFunctions();
  parse::registerTypeResolver();

  constexpr const char* kTpchConnectorId = "tpch-q6";
  const std::string exchangeId = "tpch_q6_exchange";
  connector::tpch::TpchConnectorFactory factory;
  auto tpchConnector =
      factory.newConnector(kTpchConnectorId, std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>()));
  connector::registerConnector(tpchConnector);

  core::PlanNodeId scanId;
  auto plan = PlanBuilder()
                  .tpchTableScan(tpch::Table::TBL_LINEITEM, {"l_shipdate", "l_discount", "l_quantity", "l_revenue"}, 1)
                  .capturePlanNodeId(scanId)
                  .filter("l_shipdate >= 19940101")
                  .filter("l_shipdate < 19950101")
                  .filter("l_discount >= 5")
                  .filter("l_discount <= 7")
                  .filter("l_quantity < 24")
                  .partialAggregation({}, {"sum(l_revenue) AS revenue_x10000"})
                  .localPartition(exchangeId)
                  .localMerge(exchangeId)
                  .finalAggregation({}, {"sum(revenue_x10000) AS revenue_x10000"})
                  .planNode();

  auto runtime = std::make_shared<core::ExecutionRuntime>();
  auto queryCtx = core::QueryCtx::create(runtime);
  auto task = facebook::velox::exec::Task::create("tpch_q6", plan, queryCtx);

  const auto numSplits = std::max(1u, std::thread::hardware_concurrency());
  for (uint32_t part = 0; part < numSplits; ++part) {
    task->addSplit(
        scanId,
        facebook::velox::exec::Split(std::make_shared<connector::tpch::TpchConnectorSplit>(
            kTpchConnectorId,
            true,
            tpch::Table::TBL_LINEITEM,
            1,
            static_cast<int>(part),
            static_cast<int>(numSplits))));
  }
  task->noMoreSplits(scanId);

  RowVectorPtr result;
  while (auto batch = task->next()) {
    result = std::move(batch);
  }

  connector::unregisterConnector(kTpchConnectorId);

  VELOX_CHECK(result != nullptr, "TPC-H Q6 returned no rows");
  VELOX_CHECK_EQ(result->size(), 1);

  auto revenue = std::dynamic_pointer_cast<SimpleVector<int64_t>>(result->childAt(0));
  VELOX_CHECK(revenue != nullptr, "TPC-H Q6 expected BIGINT revenue result");

  constexpr int64_t kExpectedRevenueX10000 = 1231410782283;
  VELOX_CHECK_EQ(revenue->valueAt(0), kExpectedRevenueX10000);

  const auto whole = revenue->valueAt(0) / 10000;
  const auto fraction = revenue->valueAt(0) % 10000;
  const double revenueValue = static_cast<double>(revenue->valueAt(0)) / 10000.0;

  if (!answerOutputPath.empty()) {
    std::ofstream out(answerOutputPath, std::ios::trunc);
    VELOX_CHECK(out.is_open(), "Failed to open answer output file: {}", answerOutputPath);
    out << "revenue\n";
    out << std::fixed << std::setprecision(2) << revenueValue << '\n';
  }

  std::cout << "Q6 revenue_x10000: " << revenue->valueAt(0) << std::endl;
  std::cout << "Q6 revenue: " << whole << '.' << std::setw(4) << std::setfill('0') << fraction << std::endl;
  return 0;
}
