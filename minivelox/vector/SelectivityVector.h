#pragma once

#include "common/base/Exceptions.h"
#include "vector/TypeAliases.h"
#include <vector>
#include <algorithm>
#include <cstring>

namespace facebook::velox {

class SelectivityVector {
public:
    explicit SelectivityVector(vector_size_t size, bool allSelected = true) 
        : size_(size) {
        // Round up to 64-bit words
        int64_t numWords = (size + 63) / 64;
        bits_.resize(numWords, allSelected ? ~0ULL : 0ULL);
        if (allSelected && size % 64 != 0) {
            // Clear trailing bits of the last word
            bits_.back() &= (~0ULL >> (64 - (size % 64)));
        }
    }

    bool isSelected(vector_size_t idx) const {
        return (bits_[idx / 64] >> (idx % 64)) & 1;
    }

    void setValid(vector_size_t idx, bool valid) {
        if (valid) {
            bits_[idx / 64] |= (1ULL << (idx % 64));
        } else {
            bits_[idx / 64] &= ~(1ULL << (idx % 64));
        }
    }
    
    vector_size_t size() const { return size_; }
    
    // Iterator helper (simplified)
    template<typename Func>
    void applyToSelected(Func&& func) const {
        for (vector_size_t i = 0; i < size_; ++i) {
            if (isSelected(i)) {
                func(i);
            }
        }
    }

private:
    vector_size_t size_;
    std::vector<uint64_t> bits_;
};

} // namespace facebook::velox
