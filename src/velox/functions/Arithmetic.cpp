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

bool isDoubleOutput(const TypePtr& outputType, const std::vector<VectorPtr>& args) {
  if (outputType && outputType->kind() == TypeKind::DOUBLE) {
    return true;
  }
  for (const auto& arg : args) {
    if (arg->type()->kind() == TypeKind::DOUBLE) {
      return true;
    }
  }
  return false;
}

template <typename Op>
void applyBinaryNumeric(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    const TypePtr& outputType,
    EvalCtx& context,
    VectorPtr& result,
    Op op) {
  if (isDoubleOutput(outputType, args)) {
    auto flat = std::make_shared<FlatVector<double>>(
        context.pool(),
        DOUBLE(),
        nullptr,
        rows.size(),
        AlignedBuffer::allocate(rows.size() * sizeof(double), context.pool()));
    auto* raw = flat->mutableRawValues();
    rows.applyToSelected([&](vector_size_t i) { raw[i] = op(readNumeric(args[0], i), readNumeric(args[1], i)); });
    result = flat;
    return;
  }

  auto flat =
      std::make_shared<FlatVector<int64_t>>(context.pool(), BIGINT(), nullptr, rows.size(),
                                            AlignedBuffer::allocate(rows.size() * sizeof(int64_t), context.pool()));
  auto* raw = flat->mutableRawValues();
  rows.applyToSelected(
      [&](vector_size_t i) { raw[i] = static_cast<int64_t>(op(readNumeric(args[0], i), readNumeric(args[1], i))); });
  result = flat;
}

} // namespace

class PlusFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args, const TypePtr &outputType, EvalCtx &context,
             VectorPtr &result) const override {
    applyBinaryNumeric(rows, args, outputType, context, result, [](double left, double right) { return left + right; });
  }
};

class MinusFunction : public VectorFunction {
public:
  void apply(const SelectivityVector& rows, std::vector<VectorPtr>& args, const TypePtr& outputType, EvalCtx& context,
             VectorPtr& result) const override {
    applyBinaryNumeric(rows, args, outputType, context, result, [](double left, double right) { return left - right; });
  }
};

class MultiplyFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args, const TypePtr &outputType, EvalCtx &context,
             VectorPtr &result) const override {
    applyBinaryNumeric(
        rows, args, outputType, context, result, [](double left, double right) { return left * right; });
  }
};

class DivideFunction : public VectorFunction {
public:
  void apply(const SelectivityVector& rows, std::vector<VectorPtr>& args, const TypePtr& outputType, EvalCtx& context,
             VectorPtr& result) const override {
    applyBinaryNumeric(rows, args, outputType, context, result, [](double left, double right) { return left / right; });
  }
};

class ModFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args, const TypePtr &outputType, EvalCtx &context,
             VectorPtr &result) const override {
    auto flat =
        std::make_shared<FlatVector<int64_t>>(context.pool(), BIGINT(), nullptr, rows.size(),
                                              AlignedBuffer::allocate(rows.size() * sizeof(int64_t), context.pool()));
    auto *raw = flat->mutableRawValues();

    rows.applyToSelected([&](vector_size_t i) {
      raw[i] = static_cast<int64_t>(readNumeric(args[0], i)) % static_cast<int64_t>(readNumeric(args[1], i));
    });
    result = flat;
  }
};

void registerArithmeticFunctions() {
  exec::registerFunction("plus", std::make_shared<PlusFunction>());
  exec::registerFunction("minus", std::make_shared<MinusFunction>());
  exec::registerFunction("multiply", std::make_shared<MultiplyFunction>());
  exec::registerFunction("divide", std::make_shared<DivideFunction>());
  exec::registerFunction("mod", std::make_shared<ModFunction>());
}

} // namespace facebook::velox::functions
