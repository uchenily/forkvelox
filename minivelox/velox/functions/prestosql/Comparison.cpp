#include "velox/expression/VectorFunction.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/expression/FunctionRegistry.h"
#include "velox/buffer/Buffer.h"

namespace facebook::velox::functions::prestosql {

using namespace facebook::velox::exec;

class EqFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& outputType,
               EvalCtx& context, 
               VectorPtr& result) const override {
        auto left = std::dynamic_pointer_cast<SimpleVector<int64_t>>(args[0]);
        auto right = std::dynamic_pointer_cast<SimpleVector<int64_t>>(args[1]);
        
        auto flat = std::make_shared<FlatVector<int32_t>>(
                context.pool(), INTEGER(), nullptr, rows.size(), 
                AlignedBuffer::allocate(rows.size() * sizeof(int32_t), context.pool()));
        auto* raw = flat->mutableRawValues();
        
        if (left && right) {
             rows.applyToSelected([&](vector_size_t i) {
                 raw[i] = (left->valueAt(i) == right->valueAt(i));
             });
        } else {
             // In a real engine, we'd support other types
             // For now just implementing what was there
             // Throwing generic exception or NYI if available
             throw std::runtime_error("EqFunction only supports SimpleVector<int64> for now");
        }
        result = flat;
    }
};

void registerComparisonFunctions() {
    exec::registerFunction("eq", std::make_shared<EqFunction>());
}

}
