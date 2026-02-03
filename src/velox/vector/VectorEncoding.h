#pragma once

#include <iostream>
#include <sstream>
#include <string>

namespace facebook::velox {

namespace VectorEncoding {
enum class Simple { BIASED, CONSTANT, DICTIONARY, FLAT, SEQUENCE, ROW, MAP, FLAT_MAP, ARRAY, LAZY, FUNCTION };

inline std::ostream &operator<<(std::ostream &out, const VectorEncoding::Simple &type) {
  switch (type) {
  case VectorEncoding::Simple::BIASED:
    return out << "BIASED";
  case VectorEncoding::Simple::CONSTANT:
    return out << "CONSTANT";
  case VectorEncoding::Simple::DICTIONARY:
    return out << "DICTIONARY";
  case VectorEncoding::Simple::FLAT:
    return out << "FLAT";
  case VectorEncoding::Simple::SEQUENCE:
    return out << "SEQUENCE";
  case VectorEncoding::Simple::ROW:
    return out << "ROW";
  case VectorEncoding::Simple::MAP:
    return out << "MAP";
  case VectorEncoding::Simple::FLAT_MAP:
    return out << "FLAT_MAP";
  case VectorEncoding::Simple::ARRAY:
    return out << "ARRAY";
  case VectorEncoding::Simple::LAZY:
    return out << "LAZY";
  case VectorEncoding::Simple::FUNCTION:
    return out << "FUNCTION";
  }
  return out;
}

inline std::string mapSimpleToName(const VectorEncoding::Simple &simple) {
  std::stringstream ss;
  ss << simple;
  return ss.str();
}

} // namespace VectorEncoding
} // namespace facebook::velox
