/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <fcntl.h>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>
#include <string_view>
#include <vector>
#include <memory>
#include <sys/uio.h>

#include "folly/Executor.h"
#include "folly/Range.h"
#include "folly/futures/Future.h"
#include "folly/io/IOBuf.h"
#include "folly/container/F14Map.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileIoTracer.h"
#include "velox/common/file/Region.h"
#include "velox/common/io/IoStatistics.h"

namespace facebook::velox {

namespace filesystems::File {
class IoStats {
 public:
  IoStats() = default;
  // Stub
};
}

struct FileIoContext {
  filesystems::File::IoStats* ioStats{nullptr};
  folly::F14FastMap<std::string, std::string> fileOpts;
  std::shared_ptr<FileIoTracer> ioTracer;

  FileIoContext() = default;

  explicit FileIoContext(
      filesystems::File::IoStats* stats,
      folly::F14FastMap<std::string, std::string> fileOpts = {},
      std::shared_ptr<FileIoTracer> tracer = nullptr)
      : ioStats(stats),
        fileOpts(std::move(fileOpts)),
        ioTracer(std::move(tracer)) {}
};

class ReadFile {
 public:
  virtual ~ReadFile() = default;

  virtual std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileIoContext& context = {}) const = 0;

  virtual std::string pread(
      uint64_t offset,
      uint64_t length,
      const FileIoContext& context = {}) const;

  virtual uint64_t preadv(
      uint64_t /*offset*/,
      const std::vector<folly::Range<char*>>& /*buffers*/,
      const FileIoContext& context = {}) const;

  virtual uint64_t preadv(
      folly::Range<const common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs,
      const FileIoContext& context = {}) const;

  virtual folly::SemiFuture<uint64_t> preadvAsync(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      const FileIoContext& context = {}) const {
      // Synchronous fallback
      return folly::makeSemiFuture<uint64_t>(preadv(offset, buffers, context));
  }

  virtual bool hasPreadvAsync() const {
    return false;
  }

  virtual bool shouldCoalesce() const = 0;

  virtual uint64_t size() const = 0;

  virtual uint64_t memoryUsage() const = 0;

  virtual uint64_t bytesRead() const {
    return bytesRead_;
  }

  virtual void resetBytesRead() {
    bytesRead_ = 0;
  }

  virtual std::string getName() const = 0;

  virtual uint64_t getNaturalReadSize() const = 0;

 protected:
  mutable std::atomic<uint64_t> bytesRead_ = 0;
};

class WriteFile {
 public:
  virtual ~WriteFile() = default;

  virtual void append(std::string_view data) = 0;

  virtual void append(std::unique_ptr<folly::IOBuf> /* data */) {
    VELOX_NYI("IOBuf appending is not implemented");
  }

  virtual void write(
      const std::vector<iovec>& /* iovecs */,
      int64_t /* offset */,
      int64_t /* length */
  ) {
    VELOX_NYI("{} is not implemented", __FUNCTION__);
  }

  virtual void truncate(int64_t /* newSize */) {
    VELOX_NYI("{} is not implemented", __FUNCTION__);
  }

  virtual void flush() = 0;

  virtual void setAttributes(
      const std::unordered_map<std::string, std::string>& /* attributes */) {
    VELOX_NYI("{} is not implemented", __FUNCTION__);
  }

  virtual std::unordered_map<std::string, std::string> getAttributes() const {
    VELOX_NYI("{} is not implemented", __FUNCTION__);
  }

  virtual void close() = 0;

  virtual uint64_t size() const = 0;

  virtual const std::string getName() const {
    VELOX_NYI("{} is not implemented", __FUNCTION__);
  }
};

class InMemoryReadFile : public ReadFile {
 public:
  explicit InMemoryReadFile(std::string_view file) : file_(file) {}

  explicit InMemoryReadFile(std::string file)
      : ownedFile_(std::move(file)), file_(ownedFile_) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileIoContext& context = {}) const override;

  std::string pread(
      uint64_t offset,
      uint64_t length,
      const FileIoContext& context = {}) const override;

  uint64_t size() const final {
    return file_.size();
  }

  uint64_t memoryUsage() const final {
    return size();
  }

  void setShouldCoalesce(bool shouldCoalesce) {
    shouldCoalesce_ = shouldCoalesce;
  }
  bool shouldCoalesce() const final {
    return shouldCoalesce_;
  }

  std::string getName() const override {
    return "<InMemoryReadFile>";
  }

  uint64_t getNaturalReadSize() const override {
    return 1024;
  }

 private:
  const std::string ownedFile_;
  const std::string_view file_;
  bool shouldCoalesce_ = false;
};

class InMemoryWriteFile final : public WriteFile {
 public:
  explicit InMemoryWriteFile(std::string* file) : file_(file) {}

  void append(std::string_view data) final;
  void append(std::unique_ptr<folly::IOBuf> data) final;
  void flush() final {}
  void close() final {}
  uint64_t size() const final;

 private:
  std::string* file_;
};

class LocalReadFile final : public ReadFile {
 public:
  LocalReadFile(
      std::string_view path,
      folly::Executor* executor = nullptr,
      bool bufferIo = true);

  LocalReadFile(int32_t fd, folly::Executor* executor = nullptr);

  ~LocalReadFile();

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileIoContext& context = {}) const final;

  uint64_t size() const final;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      const FileIoContext& context = {}) const final;

  folly::SemiFuture<uint64_t> preadvAsync(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      const FileIoContext& context = {}) const override;

  bool hasPreadvAsync() const override {
    return executor_ != nullptr;
  }

  uint64_t memoryUsage() const final;

  bool shouldCoalesce() const final {
    return false;
  }

  std::string getName() const override {
    if (path_.empty()) {
      return "<LocalReadFile>";
    }
    return path_;
  }

  uint64_t getNaturalReadSize() const override {
    return 10 << 20;
  }

 private:
  void preadInternal(uint64_t offset, uint64_t length, char* pos) const;

  folly::Executor* const executor_;
  std::string path_;
  int32_t fd_;
  long size_;
};

class LocalWriteFile final : public WriteFile {
 public:
  struct Attributes {
    static constexpr std::string_view kNoCow{"write-on-copy-disabled"};
    static constexpr bool kDefaultNoCow{false};

    static bool cowDisabled(
        const std::unordered_map<std::string, std::string>& attrs);
  };

  explicit LocalWriteFile(
      std::string_view path,
      bool shouldCreateParentDirectories = false,
      bool shouldThrowOnFileAlreadyExists = true,
      bool bufferIo = true);

  ~LocalWriteFile();

  void append(std::string_view data) final;

  void append(std::unique_ptr<folly::IOBuf> data) final;

  void write(const std::vector<iovec>& iovecs, int64_t offset, int64_t length)
      final;

  void truncate(int64_t newSize) final;

  void flush() final;

  void setAttributes(
      const std::unordered_map<std::string, std::string>& attributes) final;

  std::unordered_map<std::string, std::string> getAttributes() const final;

  void close() final;

  uint64_t size() const final {
    return size_;
  }

  const std::string getName() const final {
    return path_;
  }

 private:
  int32_t fd_{-1};
  std::string path_;
  uint64_t size_{0};
  std::unordered_map<std::string, std::string> attributes_{};
  bool closed_{false};
};

} // namespace facebook::velox
