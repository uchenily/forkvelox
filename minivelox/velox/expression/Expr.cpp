#include "velox/expression/Expr.h"
#include "velox/core/Expressions.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/type/StringView.h"
#include <sstream>
#include <algorithm>
#include <cctype>
#include <unordered_map>
#include <iomanip>

namespace facebook::velox::exec {

// Base Expr implementations
std::string Expr::toString(bool recursive) const {
  if (recursive) {
    std::stringstream out;
    out << name_;
    appendInputs(out);
    return out.str();
  }
  return name_;
}

void Expr::appendInputs(std::stringstream& stream) const {
  if (!inputs_.empty()) {
    stream << "(";
    for (auto i = 0; i < inputs_.size(); ++i) {
      if (i > 0) {
        stream << ", ";
      }
      stream << inputs_[i]->toString();
    }
    stream << ")";
  }
}

namespace {

// Helpers
ExprPtr compile(const core::TypedExprPtr& expr);

class ConstantExpr : public Expr {
public:
    ConstantExpr(Variant value) 
        : Expr(resolveType(value), {}, value.toString()), value_(value) {}
        
    void eval(const SelectivityVector& rows, EvalCtx& context, VectorPtr& result) override {
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
            int64_t v = value_.value<int64_t>(); 
            auto* raw = flat->mutableRawValues();
            rows.applyToSelected([&](vector_size_t i) {
                raw[i] = (int32_t)v;
            });
            result = flat;
        } else {
             VELOX_NYI("Constant type not supported in demo yet");
        }
    }
    
private:
    static std::shared_ptr<const Type> resolveType(const Variant& v) {
        if (v.kind() == TypeKind::INTEGER) return INTEGER();
        if (v.kind() == TypeKind::BIGINT) return BIGINT();
        return BIGINT(); // Default
    }
    Variant value_;
};

class FieldReference : public Expr {
public:
    FieldReference(std::string name, std::shared_ptr<const Type> type) 
        : Expr(type, {}, name) {}
        
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
            
            if (start <= 0 || start > (int64_t)s.size()) {
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
             VELOX_NYI("EqFunction only supports SimpleVector<int64> for now");
        }
        result = flat;
    }
};

class CallExpr : public Expr {
public:
    CallExpr(std::string name, std::vector<ExprPtr> inputs, std::shared_ptr<const Type> type)
        : Expr(type, std::move(inputs), name) {}

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

void printExprTree(
    const exec::Expr& expr,
    const std::string& indent,
    bool withStats,
    std::stringstream& out,
    std::unordered_map<const exec::Expr*, uint32_t>& uniqueExprs) {
  
  auto it = uniqueExprs.find(&expr);
  if (it != uniqueExprs.end()) {
      out << indent << expr.toString(true) << " -> " << expr.type()->toString();
      out << " [CSE #" << it->second << "]" << std::endl;
      return;
  }
  
  uint32_t id = uniqueExprs.size() + 1;
  uniqueExprs.insert({&expr, id});

  const auto& stats = expr.stats();
  out << indent << expr.toString(false);
  // if (withStats) { ... } // stub stats
  out << " -> " << expr.type()->toString() << " [#" << id << "]" << std::endl;

  auto newIndent = indent + "   ";
  for (const auto& input : expr.inputs()) {
    printExprTree(*input, newIndent, withStats, out, uniqueExprs);
  }
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
  std::unordered_map<const exec::Expr*, uint32_t> uniqueExprs;
  std::stringstream out;
  for (size_t i = 0; i < exprs_.size(); ++i) {
    if (i > 0) {
      out << std::endl;
    }
    if (compact) {
      out << exprs_[i]->toString(true /*recursive*/);
    } else {
      printExprTree(*exprs_[i], "", false /*withStats*/, out, uniqueExprs);
    }
  }
  return out.str();
}

}