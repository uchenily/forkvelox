#include "velox/expression/Expr.h"
#include "velox/core/Expressions.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

namespace {

// Helpers
ExprPtr compile(const core::TypedExprPtr& expr);

class ConstantExpr : public Expr {
public:
    ConstantExpr(Variant value) : value_(value) {}
    void eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) override {
        // Simply create a FlatVector filled with value
        // Note: In real Velox, this would result in a ConstantVector.
        // Here we simulate by creating a FlatVector.
        auto pool = context.pool();
        int size = rows.size();
        
        if (value_.kind() == TypeKind::BIGINT) {
            auto flat = std::make_shared<FlatVector<int64_t>>(
                pool, BIGINT(), nullptr, size, 
                AlignedBuffer::allocate(size * sizeof(int64_t), pool));
            int64_t v = value_.value<int64_t>();
            auto* raw = flat->mutableRawValues();
            rows.applyToSelected([&](vector_size_t i) {
                raw[i] = v;
            });
            result = flat;
        } else {
             VELOX_NYI("Constant type not supported in demo yet");
        }
    }
    std::shared_ptr<const Type> type() const override { return BIGINT(); /*TODO*/ } // Mock
private:
    Variant value_;
};

class FieldReference : public Expr {
public:
    FieldReference(std::string name, std::shared_ptr<const Type> type) : name_(name), type_(type) {}
    void eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) override {
        auto row = context.row();
        auto& children = row->children();
        auto rowType = asRowType(row->type());
        auto& names = rowType->names();
        for(size_t i=0; i<names.size(); ++i) {
            if (names[i] == name_) {
                result = children[i];
                return;
            }
        }
        throw std::runtime_error("Field not found: " + name_);
    }
    std::shared_ptr<const Type> type() const override { return type_; }
private:
    std::string name_;
    std::shared_ptr<const Type> type_;
};

// Hardcoded function logic for demo
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

class CallExpr : public Expr {
public:
    CallExpr(std::string name, std::vector<ExprPtr> inputs, std::shared_ptr<const Type> type)
        : name_(name), inputs_(std::move(inputs)), type_(type) {}

    void eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) override {
        std::vector<VectorPtr> args;
        for(auto& input : inputs_) {
            VectorPtr res;
            input->eval(rows, context, res);
            args.push_back(res);
        }
        
        // Dispatch
        if (name_ == "plus") {
            PlusFunction().apply(rows, args, type_, context, result);
        } else {
             VELOX_NYI("Function " + name_);
        }
    }
    std::shared_ptr<const Type> type() const override { return type_; }
private:
    std::string name_;
    std::vector<ExprPtr> inputs_;
    std::shared_ptr<const Type> type_;
};

ExprPtr compile(const core::TypedExprPtr& expr) {
    if (auto c = std::dynamic_pointer_cast<core::ConstantTypedExpr>(expr)) {
        return std::make_shared<ConstantExpr>(c->value());
    }
    if (auto f = std::dynamic_pointer_cast<core::FieldAccessTypedExpr>(expr)) {
        return std::make_shared<FieldReference>(f->name(), f->type());
    }
    if (auto call = std::dynamic_pointer_cast<core::CallTypedExpr>(expr)) {
        std::vector<ExprPtr> args;
        for(auto& in : call->inputs()) {
            args.push_back(compile(in));
        }
        return std::make_shared<CallExpr>(call->name(), std::move(args), call->type());
    }
    VELOX_FAIL("Unknown expr type");
}

} // namespace

ExprSet::ExprSet(std::vector<std::shared_ptr<core::ITypedExpr>> sources, core::ExecCtx* execCtx) 
    : sources_(std::move(sources)), execCtx_(execCtx) {
    for(auto& source : sources_) {
        exprs_.push_back(compile(source));
    }
}

ExprSet::~ExprSet() {}

void ExprSet::eval(const SelectivityVector& rows, EvalCtx& context, std::vector<VectorPtr>& result) {
    result.resize(exprs_.size());
    for(size_t i=0; i<exprs_.size(); ++i) {
        exprs_[i]->eval(rows, context, result[i]);
    }
}

std::string ExprSet::toString(bool compact) const {
    return "ExprSet";
}

}
