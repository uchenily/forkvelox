#pragma once

#include <string>
#include <iostream>

namespace facebook::velox {

namespace VectorEncoding {
enum class Simple {
  BIASED,
  CONSTANT,
  DICTIONARY,
  FLAT,
  SEQUENCE,
  ROW,
  MAP,
  FLAT_MAP,
  ARRAY,
  LAZY,
  FUNCTION
};
}

}
