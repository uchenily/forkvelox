#pragma once

#include "type/Type.h"
#include "type/StringView.h"
#include <variant>

namespace facebook::velox {

using VariantType = std::variant<int32_t, int64_t, double, bool, std::string>;

class Variant {
public:
    Variant(int32_t v) : value_(v), kind_(TypeKind::INTEGER) {}
    Variant(int64_t v) : value_(v), kind_(TypeKind::BIGINT) {}
    Variant(double v) : value_(v), kind_(TypeKind::DOUBLE) {}
    Variant(bool v) : value_(v), kind_(TypeKind::BOOLEAN) {}
    Variant(std::string v) : value_(v), kind_(TypeKind::VARCHAR) {}
    Variant(const char* v) : value_(std::string(v)), kind_(TypeKind::VARCHAR) {}

    TypeKind kind() const { return kind_; }
    
    template <typename T>
    T value() const {
        if constexpr (std::is_same_v<T, StringView>) {
            const std::string& s = std::get<std::string>(value_);
            return StringView(s);
        } else {
            return std::get<T>(value_);
        }
    }
    
    std::string toString() const {
        // Simplified
        if (kind_ == TypeKind::INTEGER) return std::to_string(std::get<int32_t>(value_));
        if (kind_ == TypeKind::BIGINT) return std::to_string(std::get<int64_t>(value_));
        if (kind_ == TypeKind::VARCHAR) return std::get<std::string>(value_);
        return "Variant";
    }
    
    TypePtr type() const {
        switch(kind_) {
            case TypeKind::INTEGER: return IntegerType::create();
            case TypeKind::BIGINT: return BigIntType::create();
            case TypeKind::VARCHAR: return VarcharType::create();
            default: return nullptr;
        }
    }

private:
    VariantType value_;
    TypeKind kind_;
};

} // namespace facebook::velox
