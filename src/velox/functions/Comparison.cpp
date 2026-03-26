#include "velox/buffer/Buffer.h"
#include "velox/expression/FunctionRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox::functions {

using namespace facebook::velox::exec;

namespace {

double readNumeric(const VectorPtr& vec, vector_size_t row) {
  switch (vec->type()->kind()) {
    case TypeKind::BIGINT:
      return std::dynamic_pointer_cast<SimpleVector<int64_t>>(vec)->valueAt(row);
    case TypeKind::INTEGER:
      return std::dynamic_pointer_cast<SimpleVector<int32_t>>(vec)->valueAt(row);
    case TypeKind::DOUBLE:
      return std::dynamic_pointer_cast<SimpleVector<double>>(vec)->valueAt(row);
    default:
      throw std::runtime_error("Unsupported numeric type");
  }
}

template <typename Op>
void applyComparison(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    EvalCtx& context,
    VectorPtr& result,
    Op op) {
  auto flat =
      std::make_shared<FlatVector<int32_t>>(context.pool(), INTEGER(), nullptr, rows.size(),
                                            AlignedBuffer::allocate(rows.size() * sizeof(int32_t), context.pool()));
  auto* raw = flat->mutableRawValues();
  rows.applyToSelected([&](vector_size_t i) { raw[i] = op(readNumeric(args[0], i), readNumeric(args[1], i)); });
  result = flat;
}

} // namespace

class EqFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args, const TypePtr &outputType, EvalCtx &context,
             VectorPtr &result) const override {
    applyComparison(rows, args, context, result, [](double left, double right) { return left == right; });
  }
};

class NeqFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args, const TypePtr & /*outputType*/,
             EvalCtx &context, VectorPtr &result) const override {
    applyComparison(rows, args, context, result, [](double left, double right) { return left != right; });
  }
};

class LtFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args, const TypePtr & /*outputType*/,
             EvalCtx &context, VectorPtr &result) const override {
    applyComparison(rows, args, context, result, [](double left, double right) { return left < right; });
  }
};

class GtFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args, const TypePtr & /*outputType*/,
             EvalCtx &context, VectorPtr &result) const override {
    applyComparison(rows, args, context, result, [](double left, double right) { return left > right; });
  }
};

class LteFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args, const TypePtr & /*outputType*/,
             EvalCtx &context, VectorPtr &result) const override {
    applyComparison(rows, args, context, result, [](double left, double right) { return left <= right; });
  }
};

class GteFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args, const TypePtr & /*outputType*/,
             EvalCtx &context, VectorPtr &result) const override {
    applyComparison(rows, args, context, result, [](double left, double right) { return left >= right; });
  }
};

void registerComparisonFunctions() {
  exec::registerFunction("eq", std::make_shared<EqFunction>());
  exec::registerFunction("neq", std::make_shared<NeqFunction>());
  exec::registerFunction("lt", std::make_shared<LtFunction>());
  exec::registerFunction("gt", std::make_shared<GtFunction>());
  exec::registerFunction("lte", std::make_shared<LteFunction>());
  exec::registerFunction("gte", std::make_shared<GteFunction>());
}

} // namespace facebook::velox::functions
