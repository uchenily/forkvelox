#include "velox/expression/VectorFunction.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/expression/FunctionRegistry.h"
#include "velox/buffer/Buffer.h"

namespace facebook::velox::functions {

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

class NeqFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& /*outputType*/,
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
                 raw[i] = (left->valueAt(i) != right->valueAt(i));
             });
        } else {
             throw std::runtime_error("NeqFunction only supports SimpleVector<int64> for now");
        }
        result = flat;
    }
};

class LtFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& /*outputType*/,
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
                 raw[i] = (left->valueAt(i) < right->valueAt(i));
             });
        } else {
             throw std::runtime_error("LtFunction only supports SimpleVector<int64> for now");
        }
        result = flat;
    }
};

class GtFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& /*outputType*/,
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
                 raw[i] = (left->valueAt(i) > right->valueAt(i));
             });
        } else {
             throw std::runtime_error("GtFunction only supports SimpleVector<int64> for now");
        }
        result = flat;
    }
};

class LteFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& /*outputType*/,
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
                 raw[i] = (left->valueAt(i) <= right->valueAt(i));
             });
        } else {
             throw std::runtime_error("LteFunction only supports SimpleVector<int64> for now");
        }
        result = flat;
    }
};

class GteFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& /*outputType*/,
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
                 raw[i] = (left->valueAt(i) >= right->valueAt(i));
             });
        } else {
             throw std::runtime_error("GteFunction only supports SimpleVector<int64> for now");
        }
        result = flat;
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

}
