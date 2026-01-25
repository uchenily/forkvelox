/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "common/memory/Memory.h"
#include "exec/tests/utils/AssertQueryBuilder.h"
#include "exec/tests/utils/PlanBuilder.h"
#include "exec/Expr.h"
#include "functions/Registration.h"
#include "parse/Expressions.h"
#include "vector/SimpleVector.h"
#include "vector/RowVector.h"
#include <iostream>

using namespace facebook::velox;
// using namespace facebook::velox::test; 
using namespace facebook::velox::exec::test;

namespace facebook::velox::tpch {
    enum class Table { TBL_NATION, TBL_REGION };
}

class VeloxIn10MinDemo {
 public:
  const std::string kTpchConnectorId = "test-tpch";

  VeloxIn10MinDemo() {
    exec::registerAllFunctions();
  }

  ~VeloxIn10MinDemo() {
  }

  std::unique_ptr<exec::ExprSet> compileExpression(
      const std::string& expr,
      const RowTypePtr& rowType) {
      
    auto untyped = parse::DuckSqlExpressionsParser().parseExpr(expr);
    auto typed = core::Expressions::inferTypes(untyped, rowType, pool_.get());
    
    std::vector<exec::ExprPtr> exprs = {typed};
    execCtx_ = std::make_unique<core::ExecCtx>(pool_.get(), nullptr);
    return std::make_unique<exec::ExprSet>(std::move(exprs), execCtx_.get());
  }

  VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& input) {
    // exec::EvalCtx context(execCtx_.get(), &exprSet, input.get()); // Error: input.get() is pointer
    exec::EvalCtx context(execCtx_.get(), &exprSet, input); // Pass shared_ptr

    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result(1);
    exprSet.eval(rows, context, result);
    return result[0];
  }

  exec::Split makeTpchSplit() const {
      return exec::Split(nullptr); 
  }

  void run();

  std::unique_ptr<memory::MemoryPool> pool_{memory::MemoryManager::getInstance().addRootPool()};
  std::unique_ptr<core::ExecCtx> execCtx_;
};

void VeloxIn10MinDemo::run() {
  auto a = makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6}, pool_.get());
  auto b = makeFlatVector<int64_t>({0, 5, 10, 15, 20, 25, 30}, pool_.get());
  // Removed <std::string> to use overload
  auto dow = makeFlatVector( 
      std::vector<std::string>{"monday",
       "tuesday",
       "wednesday",
       "thursday",
       "friday",
       "saturday",
       "sunday"}, pool_.get());

  auto data = makeRowVector({"a", "b", "dow"}, {a, b, dow}, pool_.get());

  std::cout << std::endl
            << "> vectors a, b, dow: " << data->toString() << std::endl;

  auto exprSet = compileExpression("a + b", asRowType(data->type()));

  std::cout << std::endl << "> 'a + b' expression:" << std::endl;
  std::cout << exprSet->toString(false) << std::endl;

  auto c = evaluate(*exprSet, data);

  auto abc = makeRowVector({"a", "b", "c"}, {a, b, c}, pool_.get());

  std::cout << std::endl << "> a, b, a + b: " << abc->toString() << std::endl;

  exprSet = compileExpression("2 * a + b % 3", asRowType(data->type()));

  std::cout << std::endl << "> '2 * a + b % 3' expression:" << std::endl;
  std::cout << exprSet->toString(false) << std::endl;

  auto d = evaluate(*exprSet, data);

  auto abd = makeRowVector({"a", "b", "d"}, {a, b, d}, pool_.get());

  std::cout << std::endl
            << "> a, b, 2 * a + b % 3: " << abd->toString() << std::endl;

  exprSet = compileExpression(
      "concat(upper(substr(dow, 1, 1)), substr(dow, 2, 2))",
      asRowType(data->type()));

  std::cout << std::endl
            << "> '3-letter prefix with first letter capitalized' expression:"
            << std::endl;
  std::cout << exprSet->toString(false) << std::endl;

  auto shortDow = evaluate(*exprSet, data);
  std::cout << std::endl
            << "> short days of week: " << shortDow->toString() << std::endl;

  auto plan = PlanBuilder(nullptr, pool_.get())
                  .values({data})
                  .singleAggregation(
                      {},
                      {"sum(a) AS sum_a",
                       "avg(a) AS avg_a",
                       "sum(b) AS sum_b",
                       "avg(b) AS avg_b"})
                  .planNode();

  auto sumAvg = AssertQueryBuilder(plan).copyResults(pool_.get());

  std::cout << std::endl
            << "> sum and average for a and b: " << sumAvg->toString()
            << std::endl;

  plan = PlanBuilder(nullptr, pool_.get()).values({data}).orderBy({"a DESC"}, false).planNode();

  auto sorted = AssertQueryBuilder(plan).copyResults(pool_.get());

  std::cout << std::endl
            << "> data sorted on 'a' in descending order: "
            << sorted->toString() << std::endl;

  plan = PlanBuilder(nullptr, pool_.get()).values({data}).topN({"a DESC"}, 3, false).planNode();

  auto top3 = AssertQueryBuilder(plan).copyResults(pool_.get());

  std::cout << std::endl
            << "> top 3 rows as sorted on 'a' in descending order: "
            << top3->toString() << std::endl;

  plan = PlanBuilder(nullptr, pool_.get()).values({data}).filter("a % 2 == 0").planNode();

  auto evenA = AssertQueryBuilder(plan).copyResults(pool_.get());

  std::cout << std::endl
            << "> rows with even values of 'a': " << evenA->toString()
            << std::endl;

  plan = PlanBuilder(nullptr, pool_.get())
             .tpchTableScan(
                 (int)tpch::Table::TBL_NATION,
                 {"n_nationkey", "n_name"},
                 1)
             .planNode();

  auto nations =
      AssertQueryBuilder(plan).split(makeTpchSplit()).copyResults(pool_.get());

  std::cout << std::endl
            << "> first 10 rows from TPC-H nation table: "
            << nations->toString() << std::endl; 

  // Join
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId nationScanId;
  core::PlanNodeId regionScanId;
  plan = PlanBuilder(planNodeIdGenerator, pool_.get())
             .tpchTableScan(
                 (int)tpch::Table::TBL_NATION, {"n_regionkey"}, 1)
             .capturePlanNodeId(nationScanId)
             .hashJoin(
                 {"n_regionkey"},
                 {"r_regionkey"},
                 PlanBuilder(planNodeIdGenerator, pool_.get())
                     .tpchTableScan(
                         (int)tpch::Table::TBL_REGION,
                         {"r_regionkey", "r_name"},
                         1)
                     .capturePlanNodeId(regionScanId)
                     .planNode(),
                 "", 
                 {"r_name"})
             .singleAggregation({"r_name"}, {"count(1) AS nation_cnt"})
             .orderBy({"r_name"}, false)
             .planNode();

  auto nationCnt = AssertQueryBuilder(plan)
                       .split(nationScanId, makeTpchSplit())
                       .split(regionScanId, makeTpchSplit())
                       .copyResults(pool_.get());

  std::cout << std::endl
            << "> number of nations per region in TPC-H: "
            << nationCnt->toString() << std::endl;
}

int main(int argc, char** argv) {
  VeloxIn10MinDemo demo;
  demo.run();
}