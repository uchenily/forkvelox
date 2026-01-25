#pragma once

#include <memory>
#include <string>

#include "velox/common/file/LocalFileSystem.h"

namespace facebook::velox::filesystems {

void registerFileSystem(
    const std::string& scheme,
    std::shared_ptr<FileSystem> fileSystem);

void registerLocalFileSystem();

std::shared_ptr<FileSystem> getFileSystem(const std::string& path);

std::unique_ptr<ReadFile> openFileForRead(const std::string& path);

} // namespace facebook::velox::filesystems
