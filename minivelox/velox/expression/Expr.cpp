#include "velox/expression/Expr.h"
#include "velox/core/Expressions.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/type/StringView.h"
#include <sstream>
#include <algorithm>
#include <cctype>

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
        } else if (value_.kind() == TypeKind::INTEGER) {
            auto flat = std::make_shared<FlatVector<int32_t>>(
                pool, INTEGER(), nullptr, size, 
                AlignedBuffer::allocate(size * sizeof(int32_t), pool));
            int64_t v = value_.value<int64_t>(); // Variant stores int32 as int64 currently
            auto* raw = flat->mutableRawValues();
            rows.applyToSelected([&](vector_size_t i) {
                raw[i] = (int32_t)v;
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

class MultiplyFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& outputType,
               EvalCtx& context, 
               VectorPtr& result) const override {
        // Simplified: assume args[0] is constant 2, args[1] is vector 'a' OR both vectors
        // The parser stub will produce Constant(2) and Field(a).
        // ConstantExpr evaluates to FlatVector(filled).
        // So both are vectors.
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

class SubstrFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& outputType,
               EvalCtx& context, 
               VectorPtr& result) const override {
        auto input = std::static_pointer_cast<FlatVector<StringView>>(args[0]);
        auto startVec = std::static_pointer_cast<SimpleVector<int64_t>>(args[1]);
        auto lenVec = std::static_pointer_cast<SimpleVector<int64_t>>(args[2]);
        
        // Prepare output
        std::vector<std::string> results(rows.size());
        rows.applyToSelected([&](vector_size_t i) {
            StringView sv = input->valueAt(i);
            std::string_view s = sv;
            int64_t start = startVec->valueAt(i); // 1-based
            int64_t len = lenVec->valueAt(i);
            
            if (start <= 0 || start > s.size()) {
                results[i] = ""; 
            } else {
                size_t available = s.size() - (start - 1);
                size_t extract = std::min((size_t)len, available);
                results[i] = std::string(s.substr(start - 1, extract));
            }
        });
        
        auto pool = context.pool();
        size_t totalLen = 0;
        for(const auto& s : results) totalLen += s.size();
        
        auto dataBuffer = AlignedBuffer::allocate(totalLen, pool);
        char* bufPtr = dataBuffer->asMutable<char>();
        auto values = AlignedBuffer::allocate(rows.size() * sizeof(StringView), pool);
        auto* rawValues = values->asMutable<StringView>();
        
        size_t offset = 0;
        rows.applyToSelected([&](vector_size_t i) {
            if (!results[i].empty()) {
                std::memcpy(bufPtr + offset, results[i].data(), results[i].size());
                rawValues[i] = StringView(bufPtr + offset, results[i].size());
                offset += results[i].size();
            } else {
                rawValues[i] = StringView();
            }
        });
        
        auto vec = std::make_shared<FlatVector<StringView>>(pool, VARCHAR(), nullptr, rows.size(), values);
        vec->addStringBuffer(dataBuffer);
        result = vec;
    }
};

class UpperFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& outputType,
               EvalCtx& context, 
               VectorPtr& result) const override {
        auto input = std::dynamic_pointer_cast<FlatVector<StringView>>(args[0]);
        
        std::vector<std::string> results(rows.size());
        rows.applyToSelected([&](vector_size_t i) {
            StringView sv = input->valueAt(i);
            std::string s = (std::string)sv;
            std::transform(s.begin(), s.end(), s.begin(), ::toupper);
            results[i] = s;
        });
        
        auto pool = context.pool();
        size_t totalLen = 0;
        for(const auto& s : results) totalLen += s.size();
        
        auto dataBuffer = AlignedBuffer::allocate(totalLen, pool);
        char* bufPtr = dataBuffer->asMutable<char>();
        auto values = AlignedBuffer::allocate(rows.size() * sizeof(StringView), pool);
        auto* rawValues = values->asMutable<StringView>();
        
        size_t offset = 0;
        rows.applyToSelected([&](vector_size_t i) {
            std::memcpy(bufPtr + offset, results[i].data(), results[i].size());
            rawValues[i] = StringView(bufPtr + offset, results[i].size());
            offset += results[i].size();
        });
        
        auto vec = std::make_shared<FlatVector<StringView>>(pool, VARCHAR(), nullptr, rows.size(), values);
        vec->addStringBuffer(dataBuffer);
        result = vec;
    }
};

class ConcatFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& outputType,
               EvalCtx& context, 
               VectorPtr& result) const override {
        auto left = std::dynamic_pointer_cast<FlatVector<StringView>>(args[0]);
        auto right = std::dynamic_pointer_cast<FlatVector<StringView>>(args[1]);
        
        std::vector<std::string> results(rows.size());
        rows.applyToSelected([&](vector_size_t i) {
            std::string s1 = (std::string)left->valueAt(i);
            std::string s2 = (std::string)right->valueAt(i);
            results[i] = s1 + s2;
        });
        
        auto pool = context.pool();
        size_t totalLen = 0;
        for(const auto& s : results) totalLen += s.size();
        
        auto dataBuffer = AlignedBuffer::allocate(totalLen, pool);
        char* bufPtr = dataBuffer->asMutable<char>();
        auto values = AlignedBuffer::allocate(rows.size() * sizeof(StringView), pool);
        auto* rawValues = values->asMutable<StringView>();
        
        size_t offset = 0;
        rows.applyToSelected([&](vector_size_t i) {
            std::memcpy(bufPtr + offset, results[i].data(), results[i].size());
            rawValues[i] = StringView(bufPtr + offset, results[i].size());
            offset += results[i].size();
        });
        
        auto vec = std::make_shared<FlatVector<StringView>>(pool, VARCHAR(), nullptr, rows.size(), values);
        vec->addStringBuffer(dataBuffer);
        result = vec;
    }
};

class EqFunction : public VectorFunction {
public:
    void apply(const SelectivityVector& rows, 
               std::vector<VectorPtr>& args, 
               const TypePtr& outputType,
               EvalCtx& context, 
               VectorPtr& result) const override {
        // Simplified: assume int64 comparison for now
        auto left = std::dynamic_pointer_cast<SimpleVector<int64_t>>(args[0]);
        auto right = std::dynamic_pointer_cast<SimpleVector<int64_t>>(args[1]);
        
        // Output is BOOLEAN (simulated as INTEGER/int32_t usually, or just int64_t for simplicity in this stub)
        // If outputType is INTEGER, use int32_t. If BIGINT, use int64_t.
        // My Type inference says INTEGER.
        
        // Let's use int32_t for boolean
        auto flat = std::make_shared<FlatVector<int32_t>>(
                context.pool(), INTEGER(), nullptr, rows.size(), 
                AlignedBuffer::allocate(rows.size() * sizeof(int32_t), context.pool()));
        auto* raw = flat->mutableRawValues();
        
        if (left && right) {
             rows.applyToSelected([&](vector_size_t i) {
                 raw[i] = (left->valueAt(i) == right->valueAt(i));
             });
        } else {
             // Handle type mismatch or other types if needed (e.g. ConstantVector which is not SimpleVector in my stub yet? 
             // Ah, ConstantExpr returns FlatVector, so it IS SimpleVector)
             VELOX_NYI("EqFunction only supports SimpleVector<int64> for now");
        }
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
        } else if (name_ == "multiply") {
            MultiplyFunction().apply(rows, args, type_, context, result);
        } else if (name_ == "mod") {
            ModFunction().apply(rows, args, type_, context, result);
        } else if (name_ == "substr") {
            SubstrFunction().apply(rows, args, type_, context, result);
        } else if (name_ == "upper") {
            UpperFunction().apply(rows, args, type_, context, result);
        } else if (name_ == "concat") {
            ConcatFunction().apply(rows, args, type_, context, result);
        } else if (name_ == "eq") {
            EqFunction().apply(rows, args, type_, context, result);
        } else {
             VELOX_NYI("Function {}", name_);
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
