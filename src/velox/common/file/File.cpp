#include "velox/common/file/FileSystems.h"
#include "velox/common/file/File.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <algorithm>

#define FOLLY_ALWAYS_INLINE inline

namespace facebook::velox {

namespace {
// Stub helper
template <typename T>
T getAttribute(
    const std::unordered_map<std::string, std::string>& attributes,
    const std::string_view& key,
    const T& defaultValue) {
    return defaultValue; 
}

FOLLY_ALWAYS_INLINE void checkNotClosed(bool closed) {
  VELOX_CHECK(!closed, "file is closed");
}
} // namespace

std::string ReadFile::pread(
    uint64_t offset,
    uint64_t length,
    const FileIoContext& context) const {
  std::string buf;
  buf.resize(length);
  auto res = pread(offset, length, buf.data(), context);
  buf.resize(res.size());
  return buf;
}

uint64_t ReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers,
    const FileIoContext& context) const {
  auto fileSize = size();
  uint64_t numRead = 0;
  if (offset >= fileSize) {
    return 0;
  }
  for (auto& range : buffers) {
    auto copySize = std::min<size_t>(range.size(), fileSize - offset);
    if (range.data() != nullptr) {
      pread(offset, copySize, range.data(), context);
    }
    offset += copySize;
    numRead += copySize;
  }
  return numRead;
}

uint64_t ReadFile::preadv(
    folly::Range<const common::Region*> regions,
    folly::Range<folly::IOBuf*> iobufs,
    const FileIoContext& context) const {
  VELOX_CHECK_EQ(regions.size(), iobufs.size());
  uint64_t length = 0;
  for (size_t i = 0; i < regions.size(); ++i) {
    const auto& region = regions[i];
    auto& output = iobufs[i];
    output = folly::IOBuf(folly::IOBuf::CREATE, region.length);
    pread(region.offset, region.length, output.writableData(), context);
    output.append(region.length);
    length += region.length;
  }
  return length;
}

std::string_view InMemoryReadFile::pread(
    uint64_t offset,
    uint64_t length,
    void* buf,
    const FileIoContext& context) const {
  bytesRead_ += length;
  memcpy(buf, file_.data() + offset, length);
  return {static_cast<char*>(buf), length};
}

std::string InMemoryReadFile::pread(
    uint64_t offset,
    uint64_t length,
    const FileIoContext& context) const {
  bytesRead_ += length;
  return std::string(file_.data() + offset, length);
}

void InMemoryWriteFile::append(std::string_view data) {
  file_->append(data);
}

void InMemoryWriteFile::append(std::unique_ptr<folly::IOBuf> data) {
  for (auto rangeIter = data->begin(); rangeIter != data->end(); ++rangeIter) {
      auto range = *rangeIter;
      file_->append(reinterpret_cast<const char*>(range.data()), range.size());
  }
}

uint64_t InMemoryWriteFile::size() const {
  return file_->size();
}

LocalReadFile::LocalReadFile(
    std::string_view path,
    folly::Executor* executor,
    bool bufferIo)
    : executor_(executor), path_(path) {
  int32_t flags = O_RDONLY;
#ifdef __linux__
  if (!bufferIo) {
    flags |= O_DIRECT;
  }
#endif // linux
  fd_ = open(path_.c_str(), flags);
  if (fd_ < 0) {
      VELOX_FAIL("open failure in LocalReadFile constructor: {} (errno: {})", path, errno);
  }
  const off_t ret = lseek(fd_, 0, SEEK_END);
  VELOX_CHECK_GE(ret, 0, "lseek failure");
  size_ = ret;
}

LocalReadFile::LocalReadFile(int32_t fd, folly::Executor* executor)
    : executor_(executor), fd_(fd) {}

LocalReadFile::~LocalReadFile() {
  if (fd_ >= 0) close(fd_);
}

void LocalReadFile::preadInternal(uint64_t offset, uint64_t length, char* pos)
    const {
  bytesRead_ += length;
  auto bytesRead = ::pread(fd_, pos, length, offset);
  VELOX_CHECK_EQ(bytesRead, length, "pread failure");
}

std::string_view LocalReadFile::pread(
    uint64_t offset,
    uint64_t length,
    void* buf,
    const FileIoContext& context) const {
  preadInternal(offset, length, static_cast<char*>(buf));
  return {static_cast<char*>(buf), length};
}

uint64_t LocalReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers,
    const FileIoContext& context) const {
  // Simple implementation using pread loop, could use preadv
  uint64_t total = 0;
  for (const auto& buf : buffers) {
      preadInternal(offset, buf.size(), buf.data());
      offset += buf.size();
      total += buf.size();
  }
  return total;
}

folly::SemiFuture<uint64_t> LocalReadFile::preadvAsync(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers,
    const FileIoContext& context) const {
   // Simplified sync fallback
    return ReadFile::preadvAsync(offset, buffers, context);
}

uint64_t LocalReadFile::size() const {
  return size_;
}

uint64_t LocalReadFile::memoryUsage() const {
  return sizeof(int32_t); // approximate
}

bool LocalWriteFile::Attributes::cowDisabled(
    const std::unordered_map<std::string, std::string>& attrs) {
  return false; // Stub
}

LocalWriteFile::LocalWriteFile(
    std::string_view path,
    bool shouldCreateParentDirectories,
    bool shouldThrowOnFileAlreadyExists,
    bool bufferIo)
    : path_(path) {
  
  // TODO: Create parent dirs
  
  int32_t flags = O_WRONLY | O_CREAT;
  if (shouldThrowOnFileAlreadyExists) {
    flags |= O_EXCL;
  }
  const int32_t mode = S_IRUSR | S_IWUSR;

  fd_ = open(path_.data(), flags, mode);
  VELOX_CHECK_GE(fd_, 0, "Cannot open or create {}: errno {}", path_, errno);

  const off_t ret = lseek(fd_, 0, SEEK_END);
  size_ = ret;
}

LocalWriteFile::~LocalWriteFile() {
  try {
    close();
  } catch (...) {}
}

void LocalWriteFile::append(std::string_view data) {
  checkNotClosed(closed_);
  const uint64_t bytesWritten = ::write(fd_, data.data(), data.size());
  VELOX_CHECK_EQ(bytesWritten, data.size(), "write failure");
  size_ += bytesWritten;
}

void LocalWriteFile::append(std::unique_ptr<folly::IOBuf> data) {
  checkNotClosed(closed_);
  for (auto rangeIter = data->begin(); rangeIter != data->end(); ++rangeIter) {
      auto range = *rangeIter;
      ::write(fd_, range.data(), range.size());
      size_ += range.size();
  }
}

void LocalWriteFile::write(
    const std::vector<iovec>& iovecs,
    int64_t offset,
    int64_t length) {
  checkNotClosed(closed_);
  const auto bytesWritten = ::pwritev(
      fd_, iovecs.data(), static_cast<ssize_t>(iovecs.size()), offset);
  VELOX_CHECK_EQ(bytesWritten, length);
  size_ = std::max<uint64_t>(size_, offset + bytesWritten);
}

void LocalWriteFile::truncate(int64_t newSize) {
  checkNotClosed(closed_);
  ::ftruncate(fd_, newSize);
  ::lseek(fd_, newSize, SEEK_SET);
  size_ = newSize;
}

void LocalWriteFile::flush() {
  checkNotClosed(closed_);
  ::fsync(fd_);
}

void LocalWriteFile::setAttributes(
    const std::unordered_map<std::string, std::string>& attributes) {
  attributes_ = attributes;
}

std::unordered_map<std::string, std::string> LocalWriteFile::getAttributes()
    const {
  return attributes_;
}

void LocalWriteFile::close() {
  if (!closed_) {
    ::close(fd_);
    closed_ = true;
  }
}

} // namespace facebook::velox
