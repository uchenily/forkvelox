#include "velox/dwio/common/ReaderFactory.h"

#include <unordered_map>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::dwio::common {
namespace {

std::unordered_map<FileFormat, std::unique_ptr<ReaderFactory>> &registry() {
  static std::unordered_map<FileFormat, std::unique_ptr<ReaderFactory>> map;
  return map;
}

} // namespace

ReaderFactory *getReaderFactory(FileFormat format) {
  auto &map = registry();
  auto it = map.find(format);
  VELOX_CHECK(it != map.end(), "ReaderFactory not registered");
  return it->second.get();
}

void registerReaderFactory(FileFormat format,
                           std::unique_ptr<ReaderFactory> factory) {
  registry()[format] = std::move(factory);
}

} // namespace facebook::velox::dwio::common
