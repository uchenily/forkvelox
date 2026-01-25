#pragma once

#include <stdexcept>
#include <string>
#include <format>
#include <iostream>

namespace facebook::velox {

class VeloxException : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class VeloxRuntimeError : public VeloxException {
    using VeloxException::VeloxException;
};

class VeloxUserError : public VeloxException {
    using VeloxException::VeloxException;
};

#define VELOX_CHECK(condition, ...) \
  if (!(condition)) { \
    throw ::facebook::velox::VeloxRuntimeError(std::format("Check failed: {} at {}:{}. {}", #condition, __FILE__, __LINE__, std::format(__VA_ARGS__))); \
  }

#define VELOX_CHECK_NOT_NULL(ptr, ...) \
    if ((ptr) == nullptr) { \
        throw ::facebook::velox::VeloxRuntimeError(std::format("Check failed: {} is null at {}:{}. {}", #ptr, __FILE__, __LINE__, std::format(__VA_ARGS__))); \
    }

#define VELOX_FAIL(...) \
    throw ::facebook::velox::VeloxRuntimeError(std::format("Fail at {}:{}. {}", __FILE__, __LINE__, std::format(__VA_ARGS__)))

#define VELOX_USER_CHECK(condition, ...) \
  if (!(condition)) { \
    throw ::facebook::velox::VeloxUserError(std::format("Check failed: {} at {}:{}. {}", #condition, __FILE__, __LINE__, std::format(__VA_ARGS__))); \
  }

} // namespace facebook::velox
