#include <folly/init/Init.h>

#include <cmath>
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
#include "velox/type/StringView.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

namespace {

std::string stringAt(const VectorPtr& vector, vector_size_t row) {
  auto simple = std::dynamic_pointer_cast<SimpleVector<StringView>>(vector);
  VELOX_CHECK(simple != nullptr, "Expected VARCHAR vector");
  const auto value = simple->valueAt(row);
  return std::string(value.data(), value.size());
}

double doubleAt(const VectorPtr& vector, vector_size_t row) {
  auto simple = std::dynamic_pointer_cast<SimpleVector<double>>(vector);
  VELOX_CHECK(simple != nullptr, "Expected DOUBLE vector");
  return simple->valueAt(row);
}

int64_t intAt(const VectorPtr& vector, vector_size_t row) {
  auto simple = std::dynamic_pointer_cast<SimpleVector<int64_t>>(vector);
  VELOX_CHECK(simple != nullptr, "Expected BIGINT vector");
  return simple->valueAt(row);
}

} // namespace

int main(int argc, char** argv) {
  folly::init::Init init{&argc, &argv, false};

  std::string answerOutputPath;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg(argv[i]);
    if (arg == "--answer-output") {
      VELOX_CHECK(i + 1 < argc, "--answer-output requires a path");
      answerOutputPath = argv[++i];
    }
  }

  memory::initializeMemoryManager(memory::MemoryManager::Options{});

  functions::registerAllScalarFunctions();
  aggregate::registerAllAggregateFunctions();
  parse::registerTypeResolver();

  constexpr const char* kTpchConnectorId = "tpch-q1";
  const std::string exchangeId = "tpch_q1_exchange";
  connector::tpch::TpchConnectorFactory factory;
  auto tpchConnector =
      factory.newConnector(kTpchConnectorId, std::make_shared<config::ConfigBase>(std::unordered_map<std::string, std::string>()));
  connector::registerConnector(tpchConnector);

  core::PlanNodeId scanId;
  auto plan = PlanBuilder()
                  .tpchTableScan(
                      tpch::Table::TBL_LINEITEM,
                      {"l_returnflag",
                       "l_linestatus",
                       "l_quantity",
                       "l_extendedprice",
                       "l_discount",
                       "l_disc_price",
                       "l_charge",
                       "l_shipdate"},
                      1)
                  .capturePlanNodeId(scanId)
                  .filter("l_shipdate <= 19980902")
                  .partialAggregation(
                      {"l_returnflag", "l_linestatus"},
                      {"sum(l_quantity) AS sum_qty",
                       "sum(l_extendedprice) AS sum_base_price",
                       "sum(l_disc_price) AS sum_disc_price",
                       "sum(l_charge) AS sum_charge",
                       "avg(l_quantity) AS avg_qty",
                       "avg(l_extendedprice) AS avg_price",
                       "avg(l_discount) AS avg_disc",
                       "count(1) AS count_order"})
                  .localPartition(exchangeId)
                  .localMerge(exchangeId)
                  .finalAggregation(
                      {"l_returnflag", "l_linestatus"},
                      {"sum(sum_qty) AS sum_qty",
                       "sum(sum_base_price) AS sum_base_price",
                       "sum(sum_disc_price) AS sum_disc_price",
                       "sum(sum_charge) AS sum_charge",
                       "avg(avg_qty) AS avg_qty",
                       "avg(avg_price) AS avg_price",
                       "avg(avg_disc) AS avg_disc",
                       "sum(count_order) AS count_order"})
                  .orderBy({"l_returnflag", "l_linestatus"}, false)
                  .planNode();

  auto runtime = std::make_shared<core::ExecutionRuntime>();
  auto queryCtx = core::QueryCtx::create(runtime);
  auto task = facebook::velox::exec::Task::create("tpch_q1", plan, queryCtx);

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

  VELOX_CHECK(result != nullptr, "TPC-H Q1 returned no rows");
  VELOX_CHECK_EQ(result->size(), 4);

  if (!answerOutputPath.empty()) {
    std::ofstream out(answerOutputPath, std::ios::trunc);
    VELOX_CHECK(out.is_open(), "Failed to open answer output file: {}", answerOutputPath);
    out << "l_returnflag|l_linestatus|sum_qty|sum_base_price|sum_disc_price|sum_charge|avg_qty|avg_price|avg_disc|count_order\n";
    for (vector_size_t row = 0; row < result->size(); ++row) {
      out << stringAt(result->childAt(0), row) << '|'
          << stringAt(result->childAt(1), row) << '|'
          << std::fixed << std::setprecision(2)
          << doubleAt(result->childAt(2), row) << '|'
          << doubleAt(result->childAt(3), row) << '|'
          << doubleAt(result->childAt(4), row) << '|'
          << doubleAt(result->childAt(5), row) << '|'
          << doubleAt(result->childAt(6), row) << '|'
          << doubleAt(result->childAt(7), row) << '|'
          << doubleAt(result->childAt(8), row) << '|'
          << intAt(result->childAt(9), row) << '\n';
    }
  }

  if (answerOutputPath.empty()) {
    std::cout << result->toString() << std::endl;
  }
  return 0;
}
