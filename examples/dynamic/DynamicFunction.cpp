#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/expression/FunctionRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/FlatVector.h"

#include <memory>
#include <vector>

namespace facebook::velox::dynamic_example {

class DynamicFunction : public exec::VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args, const TypePtr &outputType,
             exec::EvalCtx &context, VectorPtr &result) const override {
    VELOX_USER_CHECK(args.empty(), "dynamic() expects no arguments");

    auto flat = std::make_shared<FlatVector<int64_t>>(
        context.pool(), BIGINT(), nullptr, rows.size(),
        AlignedBuffer::allocate(rows.size() * sizeof(int64_t), context.pool()));
    auto *raw = flat->mutableRawValues();

    rows.applyToSelected([&](vector_size_t i) { raw[i] = 123; });
    result = flat;
  }
};

} // namespace facebook::velox::dynamic_example

extern "C" {
void registerExtensions() {
  facebook::velox::exec::registerFunction(
      "dynamic", std::make_shared<facebook::velox::dynamic_example::DynamicFunction>());
}
}
