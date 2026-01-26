#pragma once
#include "folly/Executor.h"
#include <memory>
#include <string>

namespace folly {

class NamedThreadFactory {
public:
    NamedThreadFactory(const std::string& name) {}
};

} // namespace folly
