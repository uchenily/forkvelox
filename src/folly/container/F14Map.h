#pragma once
#include <unordered_map>

namespace folly {

template <typename K, typename V> using F14FastMap = std::unordered_map<K, V>;

template <typename K, typename V> using F14Map = std::unordered_map<K, V>;

} // namespace folly
