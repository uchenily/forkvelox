#include "velox/common/base/Log.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>

namespace facebook::velox {
namespace {
std::atomic<LogLevel> gMinLevel{LogLevel::kInfo};
std::mutex gLogMutex;
std::ostream* gOut = &std::cerr;
bool gUseColor = false;
bool gIncludeTimestamp = true;
bool gAutoFlush = true;

const char* levelToString(LogLevel level) {
  switch (level) {
    case LogLevel::kDebug:
      return "DEBUG";
    case LogLevel::kInfo:
      return "INFO";
    case LogLevel::kWarn:
      return "WARN";
    case LogLevel::kError:
      return "ERROR";
  }
  return "UNKNOWN";
}

const char* levelColor(LogLevel level) {
  switch (level) {
    case LogLevel::kDebug:
      return "\033[36m";
    case LogLevel::kInfo:
      return "\033[32m";
    case LogLevel::kWarn:
      return "\033[33m";
    case LogLevel::kError:
      return "\033[31m";
  }
  return "";
}

std::string formatTimestamp() {
  using namespace std::chrono;
  auto now = system_clock::now();
  auto nowSeconds = floor<seconds>(now);
  auto ms = duration_cast<milliseconds>(now - nowSeconds);
  return std::format("{:%F %T}.{:03}", nowSeconds, ms.count());
}
} // namespace

void Log::setLevel(LogLevel level) {
  gMinLevel.store(level, std::memory_order_relaxed);
}

LogLevel Log::level() {
  return gMinLevel.load(std::memory_order_relaxed);
}

bool Log::shouldLog(LogLevel level) {
  return static_cast<int>(level) >= static_cast<int>(gMinLevel.load(std::memory_order_relaxed));
}

void Log::setOutput(std::ostream* out) {
  std::lock_guard<std::mutex> guard(gLogMutex);
  gOut = out ? out : &std::cerr;
}

void Log::setUseColor(bool enable) {
  std::lock_guard<std::mutex> guard(gLogMutex);
  gUseColor = enable;
}

void Log::setIncludeTimestamp(bool enable) {
  std::lock_guard<std::mutex> guard(gLogMutex);
  gIncludeTimestamp = enable;
}

void Log::setAutoFlush(bool enable) {
  std::lock_guard<std::mutex> guard(gLogMutex);
  gAutoFlush = enable;
}

void Log::log(
    LogLevel level,
    std::source_location location,
    std::string_view message) {
  if (!shouldLog(level)) {
    return;
  }
  logImpl(level, location, std::string(message));
}

void Log::logImpl(
    LogLevel level,
    std::source_location location,
    std::string message) {
  std::string timestamp;
  std::ostream* out = nullptr;
  bool useColor = false;
  bool includeTimestamp = true;
  bool autoFlush = true;

  {
    std::lock_guard<std::mutex> guard(gLogMutex);
    out = gOut ? gOut : &std::cerr;
    useColor = gUseColor;
    includeTimestamp = gIncludeTimestamp;
    autoFlush = gAutoFlush;
  }

  if (includeTimestamp) {
    timestamp = formatTimestamp();
  }

  std::string timePrefix;
  if (!timestamp.empty()) {
    timePrefix = std::format("[{}] ", timestamp);
  }

  const char* color = useColor ? levelColor(level) : "";
  const char* reset = useColor ? "\033[0m" : "";

  std::string line = std::format(
      "{}{}[{}] {}:{} {} - {}{}",
      color,
      timePrefix,
      levelToString(level),
      location.file_name(),
      location.line(),
      location.function_name(),
      message,
      reset);

  {
    std::lock_guard<std::mutex> guard(gLogMutex);
    (*out) << line << '\n';
    if (autoFlush) {
      out->flush();
    }
  }
}

} // namespace facebook::velox
