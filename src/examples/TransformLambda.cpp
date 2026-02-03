#include <folly/init/Init.h>

#include <cstring>
#include <iostream>
#include <vector>

#include "velox/buffer/Buffer.h"
#include "velox/core/Expressions.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/functions/registration/RegistrationFunctions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class TransformLambdaExample : public VectorTestBase {
public:
  TransformLambdaExample() {
    functions::registerAllScalarFunctions();
    parse::registerTypeResolver();
  }

  core::TypedExprPtr parseExpression(const std::string &text,
                                     const RowTypePtr &rowType) {
    auto untyped = parse::DuckSqlExpressionsParser().parseExpr(text);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
  }

  std::unique_ptr<exec::ExprSet> compileExpression(const std::string &expr,
                                                   const RowTypePtr &rowType) {
    std::vector<core::TypedExprPtr> expressions = {
        parseExpression(expr, rowType)};
    return std::make_unique<exec::ExprSet>(std::move(expressions),
                                           execCtx_.get());
  }

  VectorPtr evaluate(exec::ExprSet &exprSet, const RowVectorPtr &input) {
    exec::EvalCtx context(execCtx_.get(), &exprSet, input.get());
    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result(1);
    exprSet.eval(rows, context, result);
    return result[0];
  }

  std::shared_ptr<ArrayVector>
  makeArrayVector(const std::vector<std::vector<int64_t>> &data) {
    const auto rowCount = static_cast<vector_size_t>(data.size());
    std::vector<int32_t> offsets(rowCount, 0);
    std::vector<int32_t> sizes(rowCount, 0);
    std::vector<int64_t> elements;

    int32_t nextOffset = 0;
    for (vector_size_t row = 0; row < rowCount; ++row) {
      offsets[row] = nextOffset;
      sizes[row] = static_cast<int32_t>(data[row].size());
      for (auto value : data[row]) {
        elements.push_back(value);
      }
      nextOffset += sizes[row];
    }

    auto elementsVector = makeFlatVector<int64_t>(elements);
    auto offsetsBuffer =
        AlignedBuffer::allocate(rowCount * sizeof(int32_t), pool());
    auto sizesBuffer =
        AlignedBuffer::allocate(rowCount * sizeof(int32_t), pool());
    std::memcpy(offsetsBuffer->asMutable<uint8_t>(), offsets.data(),
                offsetsBuffer->size());
    std::memcpy(sizesBuffer->asMutable<uint8_t>(), sizes.data(),
                sizesBuffer->size());

    return std::make_shared<ArrayVector>(pool(), ARRAY(BIGINT()), nullptr,
                                         rowCount, offsetsBuffer, sizesBuffer,
                                         elementsVector);
  }

  void run() {
    auto arrays = makeArrayVector({{1, 2, 3, 4}, {3, 1, 5, 6, 7}});
    auto thresholds = makeFlatVector<int64_t>({3, 4});
    auto data = makeRowVector({"a", "b"}, {arrays, thresholds});

    std::cout << std::endl
              << "> input rows (a: array(bigint), b: bigint):" << std::endl;
    std::cout << data->toString() << std::endl;

    // Lambda body must be wrapped in parentheses: x -> (x * 2).
    auto doubleExpr = compileExpression("transform(a, x -> (x * 2))",
                                        asRowType(data->type()));
    auto doubled = evaluate(*doubleExpr, data);

    std::cout << std::endl << "> transform(a, x -> (x * 2)):" << std::endl;
    std::cout << doubled->toString() << std::endl;

    auto addExpr = compileExpression("transform(a, x -> (x + b))",
                                     asRowType(data->type()));
    auto added = evaluate(*addExpr, data);

    std::cout << std::endl << "> transform(a, x -> (x + b)):" << std::endl;
    std::cout << added->toString() << std::endl;
  }
};

int main(int argc, char **argv) {
  folly::init::Init init{&argc, &argv, false};

  TransformLambdaExample example;
  example.run();
}
