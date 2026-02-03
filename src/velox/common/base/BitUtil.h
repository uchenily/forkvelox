#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

namespace facebook::velox::bits {

inline uint64_t nwords(uint64_t nbits) { return (nbits + 63) / 64; }

inline void setBit(uint64_t *bits, uint64_t index, bool value) {
  if (value) {
    bits[index / 64] |= (1ULL << (index % 64));
  } else {
    bits[index / 64] &= ~(1ULL << (index % 64));
  }
}

inline bool isBitSet(const uint64_t *bits, uint64_t index) { return bits[index / 64] & (1ULL << (index % 64)); }

inline void fillBits(uint64_t *bits, uint64_t start, uint64_t length, bool value) {
  // Naive implementation
  for (uint64_t i = 0; i < length; ++i) {
    setBit(bits, start + i, value);
  }
}

inline int32_t countBits(const uint64_t *bits, uint64_t start, uint64_t end) {
  int32_t count = 0;
  for (uint64_t i = start; i < end; ++i) {
    if (isBitSet(bits, i))
      count++;
  }
  return count;
}

} // namespace facebook::velox::bits
