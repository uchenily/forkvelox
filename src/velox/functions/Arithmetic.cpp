#include "velox/expression/VectorFunction.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/expression/FunctionRegistry.h"
#include "velox/buffer/Buffer.h"

namespace facebook::velox::functions {

using namespace facebook::velox::exec;

class PlusFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& outputType,
               EvalCtx& context, 
               VectorPtr& result) const override {
        auto left = std::dynamic_pointer_cast<SimpleVector<int64_t>>(args[0]);
        auto right = std::dynamic_pointer_cast<SimpleVector<int64_t>>(args[1]);
        
        auto flat = std::make_shared<FlatVector<int64_t>>(
                context.pool(), BIGINT(), nullptr, rows.size(), 
                AlignedBuffer::allocate(rows.size() * sizeof(int64_t), context.pool()));
        auto* raw = flat->mutableRawValues();
        
        rows.applyToSelected([&](vector_size_t i) {
             raw[i] = left->valueAt(i) + right->valueAt(i);
        });
        result = flat;
    }
};

class MultiplyFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& outputType,
               EvalCtx& context, 
               VectorPtr& result) const override {
        auto left = std::dynamic_pointer_cast<SimpleVector<int64_t>>(args[0]);
        auto right = std::dynamic_pointer_cast<SimpleVector<int64_t>>(args[1]);
        
        auto flat = std::make_shared<FlatVector<int64_t>>(
                context.pool(), BIGINT(), nullptr, rows.size(), 
                AlignedBuffer::allocate(rows.size() * sizeof(int64_t), context.pool()));
        auto* raw = flat->mutableRawValues();
        
        rows.applyToSelected([&](vector_size_t i) {
             raw[i] = left->valueAt(i) * right->valueAt(i);
        });
        result = flat;
    }
};

class ModFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& outputType,
               EvalCtx& context, 
               VectorPtr& result) const override {
        auto left = std::dynamic_pointer_cast<SimpleVector<int64_t>>(args[0]);
        auto right = std::dynamic_pointer_cast<SimpleVector<int64_t>>(args[1]);
        
        auto flat = std::make_shared<FlatVector<int64_t>>(
                context.pool(), BIGINT(), nullptr, rows.size(), 
                AlignedBuffer::allocate(rows.size() * sizeof(int64_t), context.pool()));
        auto* raw = flat->mutableRawValues();
        
        rows.applyToSelected([&](vector_size_t i) {
             raw[i] = left->valueAt(i) % right->valueAt(i);
        });
        result = flat;
    }
};

void registerArithmeticFunctions() {
    exec::registerFunction("plus", std::make_shared<PlusFunction>());
    exec::registerFunction("multiply", std::make_shared<MultiplyFunction>());
    exec::registerFunction("mod", std::make_shared<ModFunction>());
}

}
