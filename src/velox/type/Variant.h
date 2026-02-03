#pragma once
#include "velox/type/Type.h"
#include <iostream>
#include <string>
#include <variant>
#include <vector>

namespace facebook::velox {

class Variant {
public:
  using Value = std::variant<std::monostate, int64_t, double, std::string, bool>;

  Variant() : kind_(TypeKind::UNKNOWN), value_(std::monostate{}) {}
  Variant(int64_t val) : kind_(TypeKind::BIGINT), value_(val) {}
  Variant(int32_t val) : kind_(TypeKind::INTEGER), value_(static_cast<int64_t>(val)) {} // Promoted
  Variant(std::string val) : kind_(TypeKind::VARCHAR), value_(val) {}
  Variant(const char *val) : kind_(TypeKind::VARCHAR), value_(std::string(val)) {}
  Variant(TypeKind kind) : kind_(kind), value_(std::monostate{}) {} // Null of kind

  TypeKind kind() const { return kind_; }

  bool isNull() const { return std::holds_alternative<std::monostate>(value_); }

  template <typename T>
  T value() const {
    // Simplified
    if constexpr (std::is_same_v<T, int64_t>) {
      if (std::holds_alternative<int64_t>(value_))
        return std::get<int64_t>(value_);
    }
    if constexpr (std::is_same_v<T, std::string>) {
      if (std::holds_alternative<std::string>(value_))
        return std::get<std::string>(value_);
    }
    throw std::runtime_error("Wrong type access in Variant");
  }

  std::string toString() const {
    if (isNull())
      return "null";
    if (std::holds_alternative<int64_t>(value_))
      return std::to_string(std::get<int64_t>(value_));
    if (std::holds_alternative<std::string>(value_))
      return std::get<std::string>(value_);
    return "?";
  }

private:
  TypeKind kind_;
  Value value_;
};

} // namespace facebook::velox
