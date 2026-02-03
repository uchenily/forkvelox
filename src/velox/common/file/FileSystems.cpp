#include "velox/common/file/FileSystems.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/synchronization/CallOnce.h"
#include "folly/system/HardwareConcurrency.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"

#include <cstdio>
#include <filesystem>
#include <vector>

namespace facebook::velox::filesystems {

namespace {

constexpr std::string_view kFileScheme("file:");

using RegisteredFileSystems = std::vector<std::pair<
    std::function<bool(std::string_view)>,
    std::function<std::shared_ptr<FileSystem>(
        std::shared_ptr<const config::ConfigBase>, std::string_view)>>>;

RegisteredFileSystems &registeredFileSystems() {
  static RegisteredFileSystems *fss = new RegisteredFileSystems();
  return *fss;
}

} // namespace

void registerFileSystem(
    std::function<bool(std::string_view)> schemeMatcher,
    std::function<std::shared_ptr<FileSystem>(
        std::shared_ptr<const config::ConfigBase>, std::string_view)>
        fileSystemGenerator) {
  registeredFileSystems().emplace_back(schemeMatcher, fileSystemGenerator);
}

std::shared_ptr<FileSystem>
getFileSystem(std::string_view filePath,
              std::shared_ptr<const config::ConfigBase> properties) {
  const auto &filesystems = registeredFileSystems();
  for (const auto &p : filesystems) {
    if (p.first(filePath)) {
      return p.second(properties, filePath);
    }
  }
  // Fallback to local file system if no matcher?
  // For now fail as per original
  VELOX_FAIL("No registered file system matched with file path '{}'", filePath);
}

bool isPathSupportedByRegisteredFileSystems(const std::string_view &filePath) {
  const auto &filesystems = registeredFileSystems();
  for (const auto &p : filesystems) {
    if (p.first(filePath)) {
      return true;
    }
  }
  return false;
}

namespace {

folly::once_flag localFSInstantiationFlag;

class LocalFileSystem : public FileSystem {
public:
  LocalFileSystem(std::shared_ptr<const config::ConfigBase> config,
                  const FileSystemOptions &options)
      : FileSystem(config) {
    // Executor init stubbed
  }

  ~LocalFileSystem() override {}

  std::string name() const override { return "Local FS"; }

  inline std::string_view extractPath(std::string_view path) const override {
    if (path.find(kFileScheme) == 0) {
      return path.substr(kFileScheme.length());
    }
    return path;
  }

  std::unique_ptr<ReadFile>
  openFileForRead(std::string_view path, const FileOptions &options) override {
    return std::make_unique<LocalReadFile>(extractPath(path), nullptr,
                                           options.bufferIo);
  }

  std::unique_ptr<WriteFile>
  openFileForWrite(std::string_view path, const FileOptions &options) override {
    return std::make_unique<LocalWriteFile>(
        extractPath(path), options.shouldCreateParentDirectories,
        options.shouldThrowOnFileAlreadyExists, options.bufferIo);
  }

  void remove(std::string_view path) override {
    auto file = extractPath(path);
    std::remove(std::string(file).c_str());
  }

  void rename(std::string_view oldPath, std::string_view newPath,
              bool overwrite) override {
    auto oldFile = extractPath(oldPath);
    auto newFile = extractPath(newPath);
    ::rename(std::string(oldFile).c_str(), std::string(newFile).c_str());
  }

  bool exists(std::string_view path) override {
    const auto file = extractPath(path);
    return std::filesystem::exists(file);
  }

  bool isDirectory(std::string_view path) const override {
    const auto file = extractPath(path);
    return std::filesystem::is_directory(file);
  }

  virtual std::vector<std::string> list(std::string_view path) override {
    auto directoryPath = extractPath(path);
    const std::filesystem::path folder{directoryPath};
    std::vector<std::string> filePaths;
    if (std::filesystem::exists(folder) &&
        std::filesystem::is_directory(folder)) {
      for (auto const &entry : std::filesystem::directory_iterator{folder}) {
        filePaths.push_back(entry.path());
      }
    }
    return filePaths;
  }

  void mkdir(std::string_view path, const DirectoryOptions &options) override {
    std::filesystem::create_directories(path);
  }

  void rmdir(std::string_view path) override {
    std::filesystem::remove_all(path);
  }

  static std::function<bool(std::string_view)> schemeMatcher() {
    return [](std::string_view filePath) {
      if (filePath.find(kFileScheme) == 0) {
        return true;
      }
      if (filePath.find("/") == 0) {
        return true;
      }
      // Relative paths starting with . or ..
      if (filePath.find(".") == 0) {
        return true;
      }
      // Assume local if no scheme is present (no colon)
      if (filePath.find(":") == std::string_view::npos) {
        return true;
      }
      return false;
    };
  }

  static std::function<std::shared_ptr<FileSystem>(
      std::shared_ptr<const config::ConfigBase>, std::string_view)>
  fileSystemGenerator(const FileSystemOptions &options) {
    return [options](std::shared_ptr<const config::ConfigBase> properties,
                     std::string_view filePath) {
      static std::shared_ptr<FileSystem> lfs;
      folly::call_once(localFSInstantiationFlag, [properties, options]() {
        lfs = std::make_shared<LocalFileSystem>(properties, options);
      });
      return lfs;
    };
  }
};
} // namespace

void registerLocalFileSystem(const FileSystemOptions &options) {
  registerFileSystem(LocalFileSystem::schemeMatcher(),
                     LocalFileSystem::fileSystemGenerator(options));
}
} // namespace facebook::velox::filesystems
