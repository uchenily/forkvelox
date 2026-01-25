#pragma once

#include "velox/common/file/FileSystem.h"

namespace facebook::velox {

class LocalReadFile : public ReadFile {
public:
  explicit LocalReadFile(std::string path) : path_(std::move(path)) {}

  const std::string& path() const override { return path_; }
  std::unique_ptr<std::istream> open() const override;

private:
  std::string path_;
};

} // namespace facebook::velox

namespace facebook::velox::filesystems {

class LocalFileSystem : public FileSystem {
public:
  std::unique_ptr<ReadFile> openFileForRead(
      const std::string& path) const override;
};

} // namespace facebook::velox::filesystems
