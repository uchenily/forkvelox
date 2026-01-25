#pragma once

#include "velox/vector/BaseVector.h"
#include <sstream>

namespace facebook::velox {

template <typename T>
class SimpleVector : public BaseVector {
public:
    using BaseVector::BaseVector;
    using BaseVector::toString;
    
    virtual T valueAt(vector_size_t index) const = 0;
    
    std::string toString(vector_size_t index) const override {
        if (isNullAt(index)) return "null";
        std::stringstream ss;
        ss << valueAt(index);
        return ss.str();
    }
};

}
