#include "velox/common/file/FileSystems.h"

#include <cctype>
#include <unordered_map>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::filesystems {
namespace {

std::unordered_map<std::string, std::shared_ptr<FileSystem>>& registry() {
  static std::unordered_map<std::string, std::shared_ptr<FileSystem>> map;
  return map;
}

std::string normalizeScheme(std::string scheme) {
  for (auto& c : scheme) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return scheme;
}

std::string extractScheme(const std::string& path) {
  auto pos = path.find("://");
  if (pos != std::string::npos) {
    return normalizeScheme(path.substr(0, pos));
  }
  pos = path.find(':');
  if (pos != std::string::npos) {
    return normalizeScheme(path.substr(0, pos));
  }
  return "";
}

std::string stripScheme(const std::string& path) {
  auto pos = path.find("://");
  if (pos != std::string::npos) {
    return path.substr(pos + 3);
  }
  pos = path.find(':');
  if (pos != std::string::npos) {
    return path.substr(pos + 1);
  }
  return path;
}

} // namespace

void registerFileSystem(
    const std::string& scheme,
    std::shared_ptr<FileSystem> fileSystem) {
  VELOX_CHECK(fileSystem != nullptr, "FileSystem must not be null");
  registry()[normalizeScheme(scheme)] = std::move(fileSystem);
}

void registerLocalFileSystem() {
  auto local = std::make_shared<LocalFileSystem>();
  registerFileSystem("file", local);
  registerFileSystem("", local);
}

std::shared_ptr<FileSystem> getFileSystem(const std::string& path) {
  auto scheme = extractScheme(path);
  auto it = registry().find(scheme);
  VELOX_CHECK(it != registry().end(), "FileSystem not registered");
  return it->second;
}

std::unique_ptr<ReadFile> openFileForRead(const std::string& path) {
  auto fs = getFileSystem(path);
  return fs->openFileForRead(stripScheme(path));
}

} // namespace facebook::velox::filesystems
