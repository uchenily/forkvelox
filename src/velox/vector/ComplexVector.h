#pragma once

#include "velox/vector/BaseVector.h"

namespace facebook::velox {

class RowVector : public BaseVector {
public:
    using BaseVector::toString;

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

    int32_t compare(const BaseVector* other, vector_size_t index, vector_size_t otherIndex) const override {
        // Compare child by child
        auto* otherRow = static_cast<const RowVector*>(other);
        for(size_t i=0; i<children_.size(); ++i) {
            int cmp = children_[i]->compare(otherRow->children_[i].get(), index, otherIndex);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    void copy(const BaseVector* source, vector_size_t sourceIndex, vector_size_t targetIndex) override {
        auto* srcRow = static_cast<const RowVector*>(source);
        for(size_t i=0; i<children_.size(); ++i) {
            children_[i]->copy(srcRow->children_[i].get(), sourceIndex, targetIndex);
        }
    }

private:
    std::vector<VectorPtr> children_;
};

}
