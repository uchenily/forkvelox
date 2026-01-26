#pragma once
#include <mutex>

namespace folly {
    using once_flag = std::once_flag;
    template <typename Callable, typename... Args>
    void call_once(once_flag& flag, Callable&& f, Args&&... args) {
        std::call_once(flag, std::forward<Callable>(f), std::forward<Args>(args)...);
    }
}
