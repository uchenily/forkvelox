#include "velox/common/file/LocalFileSystem.h"

#include <fstream>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

std::unique_ptr<std::istream> LocalReadFile::open() const {
  auto input = std::make_unique<std::ifstream>(path_);
  VELOX_CHECK(input->is_open(), "Failed to open file: {}", path_);
  return input;
}

} // namespace facebook::velox

namespace facebook::velox::filesystems {

std::unique_ptr<ReadFile> LocalFileSystem::openFileForRead(
    const std::string& path) const {
  return std::make_unique<LocalReadFile>(path);
}

} // namespace facebook::velox::filesystems
