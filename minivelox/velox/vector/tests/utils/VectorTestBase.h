#pragma once
#include "velox/common/memory/Memory.h"
#include "velox/core/QueryCtx.h"
#include "velox/core/ExecCtx.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/type/StringView.h"

namespace facebook::velox::test {

class VectorTestBase {
public:
    VectorTestBase() {
        memory::MemoryManager::initialize({});
        pool_ = memory::defaultMemoryPool();
        queryCtx_ = core::QueryCtx::create();
        execCtx_ = std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get());
    }

    virtual ~VectorTestBase() = default;

    memory::MemoryPool* pool() { return pool_.get(); }

    template <typename T>
    std::shared_ptr<FlatVector<T>> makeFlatVector(const std::vector<T>& data) {
        size_t size = data.size();
        size_t bytes = size * sizeof(T);
        auto buffer = AlignedBuffer::allocate(bytes, pool());
        if (size > 0) {
            std::memcpy(buffer->as_mutable_uint8_t(), data.data(), bytes);
        }
        
        std::shared_ptr<const Type> type;
        if constexpr (std::is_same_v<T, int64_t>) type = BIGINT();
        else if constexpr (std::is_same_v<T, int32_t>) type = INTEGER();
        else type = BIGINT(); // Default
        
        return std::make_shared<FlatVector<T>>(pool(), type, nullptr, size, buffer);
    }
    
    // Specialization for String
    // Since we cannot specialize the template member function easily inside the class without partial specialization (which is not allowed for functions), 
    // we use overload or specific name. The demo calls makeFlatVector<string>(...).
    // So we need valid template instantiation.
    // If T=string, sizeof(T) is 32. Copying std::string directly into buffer is BAD.
    // But wait, the demo calls `makeFlatVector<std::string>`.
    // Velox `makeFlatVector` likely returns `FlatVector<StringView>` for `std::string` input.
    // So I need a wrapper.
    
    // Using `if constexpr` in makeFlatVector to delegate.
    
    // But return type must be `FlatVector<StringView>` not `FlatVector<std::string>`.
    // So the return type depends on T.
    
    // I will simplify: I'll implement `makeFlatVector` to accept `vector<string>` and return `FlatVector<StringView>`.
    // And `makeFlatVector` accepting `vector<int64_t>` returns `FlatVector<int64_t>`.
    
    std::shared_ptr<FlatVector<StringView>> makeFlatVector(const std::vector<std::string>& data) {
        size_t size = data.size();
        auto values = AlignedBuffer::allocate(size * sizeof(StringView), pool());
        auto* rawValues = values->asMutable<StringView>();
        
        size_t totalLen = 0;
        for(const auto& s : data) totalLen += s.size();
        
        auto dataBuffer = AlignedBuffer::allocate(totalLen, pool());
        char* bufPtr = dataBuffer->asMutable<char>();
        size_t offset = 0;
        
        for(size_t i=0; i<size; ++i) {
            std::memcpy(bufPtr + offset, data[i].data(), data[i].size());
            // Use constructor that takes char* and len. StringView will handle inlining or pointing.
            // If we point, we point to `bufPtr + offset`.
            // StringView(const char* data, int32_t len)
            rawValues[i] = StringView(bufPtr + offset, data[i].size());
            offset += data[i].size();
        }
        
        auto vec = std::make_shared<FlatVector<StringView>>(pool(), VARCHAR(), nullptr, size, values);
        vec->addStringBuffer(dataBuffer);
        return vec;
    }

    std::shared_ptr<RowVector> makeRowVector(const std::vector<std::string>& names, const std::vector<VectorPtr>& children) {
        std::vector<TypePtr> types;
        for(auto& c : children) types.push_back(c->type());
        auto rowType = ROW(names, types);
        return std::make_shared<RowVector>(pool(), rowType, nullptr, children[0]->size(), children);
    }

    std::shared_ptr<memory::MemoryPool> pool_;
    std::shared_ptr<core::QueryCtx> queryCtx_;
    std::unique_ptr<core::ExecCtx> execCtx_;
};

}
