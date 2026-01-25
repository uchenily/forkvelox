#pragma once

#include <vector>
#include <cstdint>
#include <string>
#include <sstream>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

using vector_size_t = int32_t;

class SelectivityVector {
public:
    SelectivityVector(vector_size_t size, bool allSelected = true) 
        : size_(size), allSelected_(allSelected) {
        bits_.resize(bits::nwords(size), allSelected ? ~0ULL : 0);
    }

    vector_size_t size() const { return size_; }
    bool isAllSelected() const { return allSelected_; }

    bool isValid(vector_size_t index) const {
        return bits::isBitSet(bits_.data(), index);
    }

    void setValid(vector_size_t index, bool valid) {
        bits::setBit(bits_.data(), index, valid);
        allSelected_ = false; // Pessimistic
    }
    
    void resize(vector_size_t size, bool value = true) {
        size_ = size;
        bits_.resize(bits::nwords(size), value ? ~0ULL : 0);
        allSelected_ = value; // Simplification
    }
    
    template <typename Callable>
    void applyToSelected(Callable func) const {
        for (vector_size_t i = 0; i < size_; ++i) {
            if (isValid(i)) {
                func(i);
            }
        }
    }
    
    // For test
    std::string toString() const {
        std::stringstream ss;
        ss << "SelectivityVector(size=" << size_ << ", selected=";
        int count = 0;
        for(int i=0; i<size_; ++i) if(isValid(i)) count++;
        ss << count << ")";
        return ss.str();
    }

    const uint64_t* asRange() const { return bits_.data(); }

private:
    vector_size_t size_;
    std::vector<uint64_t> bits_;
    bool allSelected_;
};

}
