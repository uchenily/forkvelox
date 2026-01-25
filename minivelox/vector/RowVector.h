#pragma once

#include "vector/BaseVector.h"
#include <vector>
#include <sstream>

namespace facebook::velox {

class RowVector : public BaseVector {
public:
    RowVector(memory::MemoryPool* pool, TypePtr type, BufferPtr nulls, 
              vector_size_t length, std::vector<VectorPtr> children)
        : BaseVector(pool, std::move(type), VectorEncoding::ROW, std::move(nulls), length),
          children_(std::move(children)) {}

    const std::vector<VectorPtr>& children() const { return children_; }
    VectorPtr childAt(int32_t idx) const { return children_[idx]; }

    std::string toString(vector_size_t start, vector_size_t end) const override {
        std::stringstream ss;
        ss << "[";
        // Simplified row printing
        for (vector_size_t i = start; i < end; ++i) {
             if (i > start) ss << ", ";
             ss << "{";
             auto rowType = std::dynamic_pointer_cast<const RowType>(type_);
             for (size_t c = 0; c < children_.size(); ++c) {
                 if (c > 0) ss << ", ";
                 ss << rowType->nameOf(c) << ": ";
                 // recursive print single value? 
                 // BaseVector API doesn't have printValueAt, only toString range.
                 // Hack for demo: toString(i, i+1)
                 std::string val = children_[c]->toString(i, i+1);
                 // remove brackets []
                 if (val.size() >= 2) ss << val.substr(1, val.size()-2);
                 else ss << val;
             }
             ss << "}";
        }
        ss << "]";
        return ss.str();
    }

private:
    std::vector<VectorPtr> children_;
};

inline std::shared_ptr<RowVector> makeRowVector(
    const std::vector<std::string>& names, 
    const std::vector<VectorPtr>& children,
    memory::MemoryPool* pool = nullptr) {
    
    if (children.empty()) throw std::runtime_error("Empty children in makeRowVector");
    if (!pool) pool = children[0]->pool();

    vector_size_t length = children[0]->size();
    std::vector<TypePtr> childTypes;
    for (const auto& c : children) {
        if (c->size() != length) throw std::runtime_error("Children size mismatch");
        childTypes.push_back(c->type());
    }
    
    auto type = RowType::create(names, childTypes);
    return std::make_shared<RowVector>(pool, type, nullptr, length, children);
}

inline std::shared_ptr<const RowType> asRowType(const TypePtr& type) {
    return std::dynamic_pointer_cast<const RowType>(type);
}

} // namespace facebook::velox
