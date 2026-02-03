#pragma once

#include <format>
#include <ostream>
#include <source_location>
#include <string>
#include <string_view>

namespace facebook::velox {

enum class LogLevel { kDebug = 0, kInfo = 1, kWarn = 2, kError = 3 };

class Log {
public:
  static void setLevel(LogLevel level);
  static LogLevel level();
  static bool shouldLog(LogLevel level);

  static void setOutput(std::ostream *out);
  static void setUseColor(bool enable);
  static void setIncludeTimestamp(bool enable);
  static void setAutoFlush(bool enable);

  template <typename... Args>
  static void log(LogLevel level, std::source_location location, std::format_string<Args...> fmt, Args &&...args) {
    if (!shouldLog(level)) {
      return;
    }
    logImpl(level, location, std::format(fmt, std::forward<Args>(args)...));
  }

  static void log(LogLevel level, std::source_location location, std::string_view message);

private:
  static void logImpl(LogLevel level, std::source_location location, std::string message);
};

#define VELOX_LOG(level, ...) ::facebook::velox::Log::log((level), std::source_location::current(), __VA_ARGS__)

#define LOG_DEBUG(...)                                                                                                 \
  ::facebook::velox::Log::log(::facebook::velox::LogLevel::kDebug, std::source_location::current(), __VA_ARGS__)

#define LOG_INFO(...)                                                                                                  \
  ::facebook::velox::Log::log(::facebook::velox::LogLevel::kInfo, std::source_location::current(), __VA_ARGS__)

#define LOG_WARN(...)                                                                                                  \
  ::facebook::velox::Log::log(::facebook::velox::LogLevel::kWarn, std::source_location::current(), __VA_ARGS__)

#define LOG_ERROR(...)                                                                                                 \
  ::facebook::velox::Log::log(::facebook::velox::LogLevel::kError, std::source_location::current(), __VA_ARGS__)

} // namespace facebook::velox
