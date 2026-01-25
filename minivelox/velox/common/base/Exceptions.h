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
public:
    using VeloxException::VeloxException;
};

class VeloxUserError : public VeloxException {
public:
    using VeloxException::VeloxException;
};

namespace detail {
template<typename... Args>
[[noreturn]] void veloxCheckFail(const char* file, int line, const char* expr, std::format_string<Args...> fmt, Args&&... args) {
    std::string msg = std::format(fmt, std::forward<Args>(args)...);
    std::string fullMsg = std::format("[{}:{}] Check failed: {} ", file, line, expr, msg);
    throw VeloxRuntimeError(fullMsg);
}

// Overload for no format args
[[noreturn]] inline void veloxCheckFail(const char* file, int line, const char* expr, const char* msg) {
    std::string fullMsg = std::format("[{}:{}] Check failed: {} ", file, line, expr, msg);
    throw VeloxRuntimeError(fullMsg);
}
} // namespace detail

#define VELOX_CHECK(condition, ...) \
    do { \
        if (!(condition)) { \
             ::facebook::velox::detail::veloxCheckFail(__FILE__, __LINE__, #condition, __VA_ARGS__); \
        } \
    } while(0)

#define VELOX_FAIL(...) \
     ::facebook::velox::detail::veloxCheckFail(__FILE__, __LINE__, "FAIL", __VA_ARGS__)

#define VELOX_USER_CHECK(condition, ...) VELOX_CHECK(condition, __VA_ARGS__)
#define VELOX_NYI(...) VELOX_FAIL("Not yet implemented: " __VA_ARGS__)
#define VELOX_UNREACHABLE(...) VELOX_FAIL("Unreachable: " __VA_ARGS__)

}
