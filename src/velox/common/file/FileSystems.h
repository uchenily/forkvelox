#pragma once

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/memory/MemoryPool.h"

#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "folly/container/F14Map.h"

namespace facebook::velox {
namespace config {
// Stub ConfigBase
class ConfigBase {
public:
  virtual ~ConfigBase() = default;
};
} // namespace config
class ReadFile;
class WriteFile;
} // namespace facebook::velox

namespace facebook::velox::filesystems {

struct FileOptions {
  static constexpr std::string_view kFileCreateConfig{"file-create-config"};

  std::unordered_map<std::string, std::string> values{};
  memory::MemoryPool *pool{nullptr};
  std::optional<int64_t> fileSize{};
  bool shouldCreateParentDirectories{false};
  bool shouldThrowOnFileAlreadyExists{true};
  bool bufferIo{true};
  std::optional<std::unordered_map<std::string, std::string>> properties{
      std::nullopt};
  std::shared_ptr<std::string> extraFileInfo{nullptr};
  std::optional<int64_t> readRangeHint{std::nullopt};
  folly::F14FastMap<std::string, std::string> fileReadOps{};
};

struct DirectoryOptions : FileOptions {
  bool failIfExists{false};
  static constexpr std::string_view kMakeDirectoryConfig{
      "make-directory-config"};
};

struct FileSystemOptions {
  bool readAheadEnabled{false};
};

class FileSystem {
public:
  FileSystem(std::shared_ptr<const config::ConfigBase> config)
      : config_(std::move(config)) {}
  virtual ~FileSystem() = default;

  virtual std::string name() const = 0;

  virtual std::string_view extractPath(std::string_view path) const {
    VELOX_NYI("extractPath");
  }

  virtual std::unique_ptr<ReadFile>
  openFileForRead(std::string_view path, const FileOptions &options = {}) = 0;

  virtual std::unique_ptr<WriteFile>
  openFileForWrite(std::string_view path, const FileOptions &options = {}) = 0;

  virtual void remove(std::string_view path) = 0;

  virtual void rename(std::string_view oldPath, std::string_view newPath,
                      bool overwrite = false) = 0;

  virtual bool exists(std::string_view path) = 0;

  virtual bool isDirectory(std::string_view path) const {
    // Stub implementation
    return false;
  }

  virtual std::vector<std::string> list(std::string_view path) = 0;

  virtual void mkdir(std::string_view path,
                     const DirectoryOptions &options = {}) = 0;

  virtual void rmdir(std::string_view path) = 0;

  virtual void setDirectoryProperty(std::string_view /*path*/,
                                    const DirectoryOptions &options = {}) {
    VELOX_FAIL("setDirectoryProperty not implemented");
  }

  virtual std::optional<std::string>
  getDirectoryProperty(std::string_view /*path*/,
                       std::string_view /*propertyKey*/) {
    VELOX_FAIL("getDirectoryProperty not implemented");
  }

protected:
  std::shared_ptr<const config::ConfigBase> config_;
};

std::shared_ptr<FileSystem>
getFileSystem(std::string_view filename,
              std::shared_ptr<const config::ConfigBase> config);

bool isPathSupportedByRegisteredFileSystems(const std::string_view &filePath);

void registerFileSystem(
    std::function<bool(std::string_view)> schemeMatcher,
    std::function<std::shared_ptr<FileSystem>(
        std::shared_ptr<const config::ConfigBase>, std::string_view)>
        fileSystemGenerator);

void registerLocalFileSystem(
    const FileSystemOptions &options = FileSystemOptions());

} // namespace facebook::velox::filesystems
