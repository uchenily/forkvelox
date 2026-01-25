#pragma once

#include "velox/vector/BaseVector.h"

namespace facebook::velox {

class RowVector : public BaseVector {
public:
    RowVector(memory::MemoryPool* pool,
              std::shared_ptr<const Type> type,
              BufferPtr nulls,
              vector_size_t length,
              std::vector<VectorPtr> children)
        : BaseVector(pool, type, VectorEncoding::Simple::ROW, std::move(nulls), length),
          children_(std::move(children)) {
    }
    
    std::string toString(vector_size_t index) const override {
        if (isNullAt(index)) return "null";
        std::string s = "{";
        for(size_t i=0; i<children_.size(); ++i) {
            if (i > 0) s += ", ";
            s += children_[i]->toString(index);
        }
        s += "}";
        return s;
    }
    
    const std::vector<VectorPtr>& children() const { return children_; }
    const VectorPtr& childAt(int32_t index) const { return children_[index]; }
    
    vector_size_t childrenSize() const { return children_.size(); }

private:
    std::vector<VectorPtr> children_;
};

}
