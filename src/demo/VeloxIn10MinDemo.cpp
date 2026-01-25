#include <folly/init/Init.h>
#include <folly/system/HardwareConcurrency.h>
#include "velox/common/memory/Memory.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;

class VeloxIn10MinDemo : public VectorTestBase {
 public:
  const std::string kTpchConnectorId = "test-tpch";

  VeloxIn10MinDemo() {
    // Register Presto scalar functions.
    functions::prestosql::registerAllScalarFunctions();

    // Register Presto aggregate functions.
    aggregate::prestosql::registerAllAggregateFunctions();

    // Register type resolver with DuckDB SQL parser.
    parse::registerTypeResolver();

    // Create and register a TPC-H connector.
    connector::tpch::TpchConnectorFactory factory;
    auto tpchConnector = factory.newConnector(
        kTpchConnectorId,
        std::make_shared<config::ConfigBase>(
            std::unordered_map<std::string, std::string>()));
    connector::registerConnector(tpchConnector);
  }

  ~VeloxIn10MinDemo() {
    connector::unregisterConnector(kTpchConnectorId);
  }

  /// Parse SQL expression into a typed expression tree using DuckDB SQL parser.
  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    auto untyped = parse::DuckSqlExpressionsParser().parseExpr(text);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
  }

  /// Compile typed expression tree into an executable ExprSet.
  std::unique_ptr<exec::ExprSet> compileExpression(
      const std::string& expr,
      const RowTypePtr& rowType) {
    std::vector<core::TypedExprPtr> expressions = {
        parseExpression(expr, rowType)};
    return std::make_unique<exec::ExprSet>(
        std::move(expressions), execCtx_.get());
  }

  /// Evaluate an expression on one batch of data.
  VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& input) {
    exec::EvalCtx context(execCtx_.get(), &exprSet, input.get());

    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result(1);
    exprSet.eval(rows, context, result);
    return result[0];
  }

  /// Make TPC-H split to add to TableScan node.
  exec::Split makeTpchSplit() const {
    return exec::Split(
        std::make_shared<connector::tpch::TpchConnectorSplit>(
            kTpchConnectorId, /*cacheable=*/true, 1, 0));
  }

  /// Run the demo.
  void run();

  std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          folly::hardware_concurrency())};
  std::shared_ptr<core::QueryCtx> queryCtx_{
      core::QueryCtx::create(executor_.get())};
  // Note: VectorTestBase initializes execCtx_, but here we override or use it.
  // VectorTestBase uses queryCtx_ from itself.
  // The demo defines its own execCtx_ which shadows VectorTestBase's?
  // VectorTestBase has std::unique_ptr<core::ExecCtx> execCtx_;
  // This class also defines execCtx_. This will shadow.
  // I should remove the definition here if I want to use the one from Base,
  // OR I should conform to this definition.
  // The demo code provided has:
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
  // pool_ comes from VectorTestBase.
};

void VeloxIn10MinDemo::run() {
  // Let’s create two vectors of 64-bit integers and one vector of strings.
  auto a = makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6});
  auto b = makeFlatVector<int64_t>({0, 5, 10, 15, 20, 25, 30});
  auto dow = makeFlatVector<std::string>(
      {"monday",
       "tuesday",
       "wednesday",
       "thursday",
       "friday",
       "saturday",
       "sunday"});

  auto data = makeRowVector({"a", "b", "dow"}, {a, b, dow});

  std::cout << std::endl
            << "> vectors a, b, dow: \n" << data->toString() << std::endl;
  std::cout << data->toString() << std::endl;

  // Expressions.

  // Now, let’s compute a sum of 'a' and 'b' by evaluating 'a + b' expression.

  // First we need to parse the expression into a fully typed expression tree.
  // Then, we need to compile it into an executable ExprSet.
  auto exprSet = compileExpression("a + b", asRowType(data->type()));

  // Let's print out the ExprSet:
  std::cout << std::endl << "> 'a + b' expression:" << std::endl;
  std::cout << exprSet->toString(false /*compact*/) << std::endl;

  // Now we are ready to evaluate the expression on a batch of data.
  auto c = evaluate(*exprSet, data);

  auto abc = makeRowVector({"a", "b", "c"}, {a, b, c});

  std::cout << std::endl << "> a, b, a + b: \n" << abc->toString() << std::endl;
  std::cout << abc->toString() << std::endl;

  // Let's try a slightly more complex expression: `3 * a + sqrt(b)`.
  // Note: demo code says `2 * a + b % 3` in string but `3 * a + sqrt(b)` in comment.
  exprSet = compileExpression("2 * a + b % 3", asRowType(data->type()));

  std::cout << std::endl << "> '2 * a + b % 3' expression:" << std::endl;
  std::cout << exprSet->toString(false /*compact*/) << std::endl;

  auto d = evaluate(*exprSet, data);

  auto abd = makeRowVector({"a", "b", "d"}, {a, b, d});

  std::cout << std::endl
            << "> a, b, 2 * a + b % 3: \n" << abd->toString() << std::endl;
  std::cout << abd->toString() << std::endl;

  // Let's transform 'dow' column into a 3-letter prefix with first letter
  // capitalized, e.g. Mon, Tue, etc.
  exprSet = compileExpression(
      "concat(upper(substr(dow, 1, 1)), substr(dow, 2, 2))",
      asRowType(data->type()));

  std::cout << std::endl
            << "> '3-letter prefix with first letter capitalized' expression:"
            << std::endl;
  std::cout << exprSet->toString(false /*compact*/) << std::endl;

  auto shortDow = evaluate(*exprSet, data);
  std::cout << std::endl
            << "> short days of week: \n" << shortDow->toString() << std::endl;

  // Queries.

  // Let's compute sum and average of 'a' and 'b' by creating
  // and executing a query plan with an aggregation node.

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation(
                      {},
                      {"sum(a) AS sum_a",
                       "avg(a) AS avg_a",
                       "sum(b) AS sum_b",
                       "avg(b) AS avg_b"})
                  .planNode();

  auto sumAvg = AssertQueryBuilder(plan).copyResults(pool());

  if (sumAvg) {
      std::cout << std::endl
                << "> sum and average for a and b: \n" << sumAvg->toString()
                << std::endl;
  }

  // Now, let's sort by 'a' descending.

  plan = PlanBuilder().values({data}).orderBy({"a DESC"}, false).planNode();

  auto sorted = AssertQueryBuilder(plan).copyResults(pool());

  if (sorted) {
      std::cout << std::endl
                << "> data sorted on 'a' in descending order: \n"
                << sorted->toString() << std::endl;
  }

  // And take top 3 rows.

  plan = PlanBuilder().values({data}).topN({"a DESC"}, 3, false).planNode();

  auto top3 = AssertQueryBuilder(plan).copyResults(pool());

  if (top3) {
      std::cout << std::endl
                << "> top 3 rows as sorted on 'a' in descending order: \n"
                << top3->toString() << std::endl;
  }

  // We can also filter rows that have even values of 'a'.
  plan = PlanBuilder().values({data}).filter("a % 2 == 0").planNode();

  auto evenA = AssertQueryBuilder(plan).copyResults(pool());

  if (evenA) {
      std::cout << std::endl
                << "> rows with even values of column 'a' (a % 2 == 0): \n" << evenA->toString()
                << std::endl;
  }

  // Now, let's read some data from the TPC-H connector which generates TPC-H
  // data on the fly. We are going to read columns n_nationkey and n_name from
  // nation table and print first 10 rows.

  // nations
  plan = PlanBuilder()
             .tpchTableScan(
                 tpch::Table::TBL_NATION,
                 {"n_regionkey", "n_name"},
                 1 /*scaleFactor*/)
             .planNode();

  auto nations =
      AssertQueryBuilder(plan).split(makeTpchSplit()).copyResults(pool());

  if (nations) {
      std::cout << std::endl
                << "> TPC-H nation table: \n"
                << nations->toString() << std::endl;
  } else {
      std::cout << "> TPC-H nation table is empty!" << std::endl;
  }

  // regions
  plan = PlanBuilder()
             .tpchTableScan(
                 tpch::Table::TBL_REGION,
                 {"r_regionkey", "r_name"},
                 1 /*scaleFactor*/)
             .planNode();

  auto regions =
      AssertQueryBuilder(plan).split(makeTpchSplit()).copyResults(pool());

  if (regions) {
      std::cout << std::endl
                << "> TPC-H region table: \n"
                << regions->toString() << std::endl;
  } else {
      std::cout << "> TPC-H region table is empty!" << std::endl;
  }

  // Let's join TPC-H nation and region tables to count number of nations in
  // each region and sort results by region name. We need to use one TableScan
  // node for nations table and another for region table. We also need to
  // provide splits for each TableScan node. We are going to use two
  // PlanBuilders: one for the probe side of the join and another one for the
  // build side. We are going to use PlanNodeIdGenerator to ensure that all plan
  // nodes in the final plan have unique IDs.

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId nationScanId;
  core::PlanNodeId regionScanId;
  plan = PlanBuilder(planNodeIdGenerator)
             .tpchTableScan(
                 tpch::Table::TBL_NATION, {"n_regionkey"}, 1 /*scaleFactor*/)
             .capturePlanNodeId(nationScanId)
             .hashJoin(
                 {"n_regionkey"},
                 {"r_regionkey"},
                 PlanBuilder(planNodeIdGenerator)
                     .tpchTableScan(
                         tpch::Table::TBL_REGION,
                         {"r_regionkey", "r_name"},
                         1 /*scaleFactor*/)
                     .capturePlanNodeId(regionScanId)
                     .planNode(),
                 "", // extra filter
                 {"r_name"})
             .singleAggregation({"r_name"}, {"count(1) as nation_cnt"})
             .orderBy({"r_name"}, false)
             .planNode();

  auto nationCnt = AssertQueryBuilder(plan)
                       .split(nationScanId, makeTpchSplit())
                       .split(regionScanId, makeTpchSplit())
                       .copyResults(pool());

  if (nationCnt) {
      std::cout << std::endl
                << "> number of nations per region in TPC-H: \n"
                << nationCnt->toString() << std::endl;
  }
}

int main(int argc, char** argv) {
  folly::init::Init init{&argc, &argv, false};

  // Initializes the process-wide memory-manager with the default options.
  memory::initializeMemoryManager(memory::MemoryManager::Options{});

  VeloxIn10MinDemo demo;
  demo.run();
}
