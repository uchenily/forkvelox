#pragma once

#include <istream>
#include <memory>
#include <string>

namespace facebook::velox {

class ReadFile {
public:
  virtual ~ReadFile() = default;
  virtual const std::string& path() const = 0;
  virtual std::unique_ptr<std::istream> open() const = 0;
};

} // namespace facebook::velox

namespace facebook::velox::filesystems {

class FileSystem {
public:
  virtual ~FileSystem() = default;
  virtual std::unique_ptr<ReadFile> openFileForRead(
      const std::string& path) const = 0;
};

} // namespace facebook::velox::filesystems
