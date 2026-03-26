#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include <liburing.h>
#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

#define FOLLY_ALWAYS_INLINE inline

namespace facebook::velox {

namespace {
// Stub helper
template <typename T>
T getAttribute(const std::unordered_map<std::string, std::string> &attributes, const std::string_view &key,
               const T &defaultValue) {
  return defaultValue;
}

FOLLY_ALWAYS_INLINE void checkNotClosed(bool closed) { VELOX_CHECK(!closed, "file is closed"); }

class IoUringReadExecutor {
 public:
  struct Request {
    int fd{-1};
    off_t offset{0};
    std::vector<iovec> iovecs;
    folly::Promise<uint64_t> promise;
  };

  IoUringReadExecutor() {
    const int rc = io_uring_queue_init(256, &ring_, 0);
    VELOX_CHECK_EQ(rc, 0, "Failed to initialize io_uring: {}", rc);
    worker_ = std::thread([this]() { run(); });
  }

  ~IoUringReadExecutor() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_ = true;
      auto* sqe = io_uring_get_sqe(&ring_);
      if (sqe != nullptr) {
        io_uring_prep_nop(sqe);
        io_uring_sqe_set_data(sqe, nullptr);
        io_uring_submit(&ring_);
      }
    }
    if (worker_.joinable()) {
      worker_.join();
    }
    io_uring_queue_exit(&ring_);
  }

  folly::SemiFuture<uint64_t> readv(int fd, uint64_t offset, const std::vector<folly::Range<char*>>& buffers) {
    auto [promise, future] = folly::makePromiseContract<uint64_t>();
    auto request = std::make_unique<Request>();
    request->fd = fd;
    request->offset = static_cast<off_t>(offset);
    request->promise = std::move(promise);
    request->iovecs.reserve(buffers.size());
    for (const auto& buffer : buffers) {
      request->iovecs.push_back(iovec{buffer.data(), buffer.size()});
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      auto* sqe = io_uring_get_sqe(&ring_);
      VELOX_CHECK_NOT_NULL(sqe, "io_uring submission queue is full");
      io_uring_prep_readv(
          sqe,
          request->fd,
          request->iovecs.data(),
          static_cast<unsigned>(request->iovecs.size()),
          request->offset);
      io_uring_sqe_set_data(sqe, request.get());
      requests_.push_back(std::move(request));
      const int rc = io_uring_submit(&ring_);
      VELOX_CHECK_GE(rc, 0, "io_uring_submit failed: {}", rc);
    }
    return future;
  }

  static IoUringReadExecutor& instance() {
    static IoUringReadExecutor executor;
    return executor;
  }

 private:
  void run() {
    while (true) {
      io_uring_cqe* cqe = nullptr;
      const int rc = io_uring_wait_cqe(&ring_, &cqe);
      if (rc < 0) {
        continue;
      }
      Request* request = static_cast<Request*>(io_uring_cqe_get_data(cqe));
      const int result = cqe->res;
      io_uring_cqe_seen(&ring_, cqe);

      std::unique_ptr<Request> owned;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (request != nullptr) {
          auto it = std::find_if(requests_.begin(), requests_.end(), [&](const auto& candidate) {
            return candidate.get() == request;
          });
          if (it != requests_.end()) {
            owned = std::move(*it);
            requests_.erase(it);
          }
        }
        if (stopping_ && requests_.empty() && request == nullptr) {
          break;
        }
      }
      if (!owned) {
        continue;
      }
      if (result < 0) {
        owned->promise.setException(std::make_exception_ptr(std::runtime_error("io_uring readv failed")));
      } else {
        owned->promise.setValue(static_cast<uint64_t>(result));
      }
    }
  }

  mutable std::mutex mutex_;
  io_uring ring_{};
  std::thread worker_;
  bool stopping_{false};
  std::vector<std::unique_ptr<Request>> requests_;
};
} // namespace

std::string ReadFile::pread(uint64_t offset, uint64_t length, const FileIoContext &context) const {
  std::string buf;
  buf.resize(length);
  auto res = pread(offset, length, buf.data(), context);
  buf.resize(res.size());
  return buf;
}

uint64_t ReadFile::preadv(uint64_t offset, const std::vector<folly::Range<char *>> &buffers,
                          const FileIoContext &context) const {
  auto fileSize = size();
  uint64_t numRead = 0;
  if (offset >= fileSize) {
    return 0;
  }
  for (auto &range : buffers) {
    auto copySize = std::min<size_t>(range.size(), fileSize - offset);
    if (range.data() != nullptr) {
      pread(offset, copySize, range.data(), context);
    }
    offset += copySize;
    numRead += copySize;
  }
  return numRead;
}

uint64_t ReadFile::preadv(folly::Range<const common::Region *> regions, folly::Range<folly::IOBuf *> iobufs,
                          const FileIoContext &context) const {
  VELOX_CHECK_EQ(regions.size(), iobufs.size());
  uint64_t length = 0;
  for (size_t i = 0; i < regions.size(); ++i) {
    const auto &region = regions[i];
    auto &output = iobufs[i];
    output = folly::IOBuf(folly::IOBuf::CREATE, region.length);
    pread(region.offset, region.length, output.writableData(), context);
    output.append(region.length);
    length += region.length;
  }
  return length;
}

std::string_view InMemoryReadFile::pread(uint64_t offset, uint64_t length, void *buf,
                                         const FileIoContext &context) const {
  bytesRead_ += length;
  memcpy(buf, file_.data() + offset, length);
  return {static_cast<char *>(buf), length};
}

std::string InMemoryReadFile::pread(uint64_t offset, uint64_t length, const FileIoContext &context) const {
  bytesRead_ += length;
  return std::string(file_.data() + offset, length);
}

void InMemoryWriteFile::append(std::string_view data) { file_->append(data); }

void InMemoryWriteFile::append(std::unique_ptr<folly::IOBuf> data) {
  for (auto rangeIter = data->begin(); rangeIter != data->end(); ++rangeIter) {
    auto range = *rangeIter;
    file_->append(reinterpret_cast<const char *>(range.data()), range.size());
  }
}

uint64_t InMemoryWriteFile::size() const { return file_->size(); }

LocalReadFile::LocalReadFile(std::string_view path, folly::Executor *executor, bool bufferIo)
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

LocalReadFile::LocalReadFile(int32_t fd, folly::Executor *executor) : executor_(executor), fd_(fd) {}

LocalReadFile::~LocalReadFile() {
  if (fd_ >= 0)
    close(fd_);
}

void LocalReadFile::preadInternal(uint64_t offset, uint64_t length, char *pos) const {
  bytesRead_ += length;
  auto bytesRead = ::pread(fd_, pos, length, offset);
  VELOX_CHECK_EQ(bytesRead, length, "pread failure");
}

std::string_view LocalReadFile::pread(uint64_t offset, uint64_t length, void *buf, const FileIoContext &context) const {
  preadInternal(offset, length, static_cast<char *>(buf));
  return {static_cast<char *>(buf), length};
}

uint64_t LocalReadFile::preadv(uint64_t offset, const std::vector<folly::Range<char *>> &buffers,
                               const FileIoContext &context) const {
  // Simple implementation using pread loop, could use preadv
  uint64_t total = 0;
  for (const auto &buf : buffers) {
    preadInternal(offset, buf.size(), buf.data());
    offset += buf.size();
    total += buf.size();
  }
  return total;
}

folly::SemiFuture<uint64_t> LocalReadFile::preadvAsync(uint64_t offset,
                                                       const std::vector<folly::Range<char *>> &buffers,
                                                       const FileIoContext &context) const {
  return IoUringReadExecutor::instance().readv(fd_, offset, buffers);
}

uint64_t LocalReadFile::size() const { return size_; }

uint64_t LocalReadFile::memoryUsage() const {
  return sizeof(int32_t); // approximate
}

bool LocalWriteFile::Attributes::cowDisabled(const std::unordered_map<std::string, std::string> &attrs) {
  return false; // Stub
}

LocalWriteFile::LocalWriteFile(std::string_view path, bool shouldCreateParentDirectories,
                               bool shouldThrowOnFileAlreadyExists, bool bufferIo)
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
  } catch (...) {
  }
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

void LocalWriteFile::write(const std::vector<iovec> &iovecs, int64_t offset, int64_t length) {
  checkNotClosed(closed_);
  const auto bytesWritten = ::pwritev(fd_, iovecs.data(), static_cast<ssize_t>(iovecs.size()), offset);
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

void LocalWriteFile::setAttributes(const std::unordered_map<std::string, std::string> &attributes) {
  attributes_ = attributes;
}

std::unordered_map<std::string, std::string> LocalWriteFile::getAttributes() const { return attributes_; }

void LocalWriteFile::close() {
  if (!closed_) {
    ::close(fd_);
    closed_ = true;
  }
}

} // namespace facebook::velox
