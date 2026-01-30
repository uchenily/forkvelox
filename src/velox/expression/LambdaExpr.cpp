#include "velox/expression/LambdaExpr.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

namespace {

VectorPtr makeRepeatedVector(
    const VectorPtr& source,
    vector_size_t sourceRow,
    vector_size_t size,
    memory::MemoryPool* pool) {
    if (auto simple64 = std::dynamic_pointer_cast<SimpleVector<int64_t>>(source)) {
        auto buffer = AlignedBuffer::allocate(size * sizeof(int64_t), pool);
        auto flat = std::make_shared<FlatVector<int64_t>>(pool, source->type(), nullptr, size, buffer);
        auto* raw = flat->mutableRawValues();
        auto value = simple64->valueAt(sourceRow);
        for (vector_size_t i = 0; i < size; ++i) {
            raw[i] = value;
        }
        return flat;
    }
    if (auto simple32 = std::dynamic_pointer_cast<SimpleVector<int32_t>>(source)) {
        auto buffer = AlignedBuffer::allocate(size * sizeof(int32_t), pool);
        auto flat = std::make_shared<FlatVector<int32_t>>(pool, source->type(), nullptr, size, buffer);
        auto* raw = flat->mutableRawValues();
        auto value = simple32->valueAt(sourceRow);
        for (vector_size_t i = 0; i < size; ++i) {
            raw[i] = value;
        }
        return flat;
    }
    if (auto simpleStr = std::dynamic_pointer_cast<SimpleVector<StringView>>(source)) {
        auto buffer = AlignedBuffer::allocate(size * sizeof(StringView), pool);
        auto flat = std::make_shared<FlatVector<StringView>>(pool, source->type(), nullptr, size, buffer);
        auto* raw = flat->mutableRawValues();
        auto value = simpleStr->valueAt(sourceRow);
        for (vector_size_t i = 0; i < size; ++i) {
            raw[i] = value;
        }
        return flat;
    }
    VELOX_NYI("Capture type not supported for lambda evaluation");
}

class LambdaCallable : public Callable {
public:
    LambdaCallable(
        RowTypePtr signature,
        ExprPtr body,
        std::vector<std::string> captureNames,
        const RowVector* outerRow,
        core::ExecCtx* execCtx)
        : signature_(std::move(signature)),
          body_(std::move(body)),
          captureNames_(std::move(captureNames)),
          outerRow_(outerRow),
          execCtx_(execCtx) {
        auto outerType = asRowType(outerRow_->type());
        VELOX_CHECK_NOT_NULL(outerType, "Lambda evaluation requires a RowVector input");

        for (const auto& name : captureNames_) {
            const auto& names = outerType->names();
            bool found = false;
            for (size_t i = 0; i < names.size(); ++i) {
                if (names[i] == name) {
                    captureIndices_.push_back(static_cast<int32_t>(i));
                    captureTypes_.push_back(outerType->childAt(i));
                    found = true;
                    break;
                }
            }
            VELOX_CHECK(found, "Capture field not found: {}", name);
        }

        std::vector<std::string> names = signature_->names();
        std::vector<TypePtr> types = signature_->children();
        for (size_t i = 0; i < captureNames_.size(); ++i) {
            names.push_back(captureNames_[i]);
            types.push_back(captureTypes_[i]);
        }
        lambdaRowType_ = ROW(std::move(names), std::move(types));
    }

    void apply(
        vector_size_t topLevelRow,
        const SelectivityVector& rows,
        EvalCtx& context,
        const std::vector<VectorPtr>& args,
        VectorPtr& result) override {
        VELOX_CHECK_EQ(args.size(), signature_->size(), "Lambda parameter count mismatch");

        std::vector<VectorPtr> children = args;
        for (size_t i = 0; i < captureIndices_.size(); ++i) {
            const auto& captureVector = outerRow_->childAt(captureIndices_[i]);
            children.push_back(makeRepeatedVector(captureVector, topLevelRow, rows.size(), execCtx_->pool()));
        }

        auto lambdaRow = std::make_shared<RowVector>(
            execCtx_->pool(),
            lambdaRowType_,
            nullptr,
            rows.size(),
            std::move(children));

        EvalCtx lambdaCtx(execCtx_, nullptr, lambdaRow.get());
        body_->eval(rows, lambdaCtx, result);
    }

private:
    RowTypePtr signature_;
    ExprPtr body_;
    std::vector<std::string> captureNames_;
    std::vector<int32_t> captureIndices_;
    std::vector<TypePtr> captureTypes_;
    RowTypePtr lambdaRowType_;
    const RowVector* outerRow_;
    core::ExecCtx* execCtx_;
};

} // namespace

void LambdaExpr::eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) {
    VELOX_CHECK_NOT_NULL(context.row(), "Lambda evaluation requires a RowVector input");
    auto functions = std::make_shared<FunctionVector>(context.pool(), type_, rows.size());
    auto callable = std::make_shared<LambdaCallable>(signature_, body_, captures_, context.row(), context.execCtx());
    functions->addFunction(callable, rows);
    result = functions;
}

} // namespace facebook::velox::exec
