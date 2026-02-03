#include "velox/buffer/Buffer.h"
#include "velox/expression/FunctionRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/StringView.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SimpleVector.h"
#include <algorithm>
#include <cctype>
#include <cstring>

namespace facebook::velox::functions {

using namespace facebook::velox::exec;

class SubstrFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args,
             const TypePtr &outputType, EvalCtx &context,
             VectorPtr &result) const override {
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
    for (const auto &s : results)
      totalLen += s.size();

    auto dataBuffer = AlignedBuffer::allocate(totalLen, pool);
    char *bufPtr = dataBuffer->asMutable<char>();
    auto values =
        AlignedBuffer::allocate(rows.size() * sizeof(StringView), pool);
    auto *rawValues = values->asMutable<StringView>();

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

    auto vec = std::make_shared<FlatVector<StringView>>(
        pool, VARCHAR(), nullptr, rows.size(), values);
    vec->addStringBuffer(dataBuffer);
    result = vec;
  }
};

class UpperFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args,
             const TypePtr &outputType, EvalCtx &context,
             VectorPtr &result) const override {
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
    for (const auto &s : results)
      totalLen += s.size();

    auto dataBuffer = AlignedBuffer::allocate(totalLen, pool);
    char *bufPtr = dataBuffer->asMutable<char>();
    auto values =
        AlignedBuffer::allocate(rows.size() * sizeof(StringView), pool);
    auto *rawValues = values->asMutable<StringView>();

    size_t offset = 0;
    rows.applyToSelected([&](vector_size_t i) {
      std::memcpy(bufPtr + offset, results[i].data(), results[i].size());
      rawValues[i] = StringView(bufPtr + offset, results[i].size());
      offset += results[i].size();
    });

    auto vec = std::make_shared<FlatVector<StringView>>(
        pool, VARCHAR(), nullptr, rows.size(), values);
    vec->addStringBuffer(dataBuffer);
    result = vec;
  }
};

class ConcatFunction : public VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args,
             const TypePtr &outputType, EvalCtx &context,
             VectorPtr &result) const override {
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
    for (const auto &s : results)
      totalLen += s.size();

    auto dataBuffer = AlignedBuffer::allocate(totalLen, pool);
    char *bufPtr = dataBuffer->asMutable<char>();
    auto values =
        AlignedBuffer::allocate(rows.size() * sizeof(StringView), pool);
    auto *rawValues = values->asMutable<StringView>();

    size_t offset = 0;
    rows.applyToSelected([&](vector_size_t i) {
      std::memcpy(bufPtr + offset, results[i].data(), results[i].size());
      rawValues[i] = StringView(bufPtr + offset, results[i].size());
      offset += results[i].size();
    });

    auto vec = std::make_shared<FlatVector<StringView>>(
        pool, VARCHAR(), nullptr, rows.size(), values);
    vec->addStringBuffer(dataBuffer);
    result = vec;
  }
};

void registerStringFunctions() {
  exec::registerFunction("substr", std::make_shared<SubstrFunction>());
  exec::registerFunction("upper", std::make_shared<UpperFunction>());
  exec::registerFunction("concat", std::make_shared<ConcatFunction>());
}

} // namespace facebook::velox::functions
