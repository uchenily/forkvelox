#pragma once
#include <unordered_map>
#include <string>

namespace facebook::velox::config {
    class ConfigBase {
    public:
        ConfigBase(std::unordered_map<std::string, std::string> values) : values_(std::move(values)) {}
    private:
        std::unordered_map<std::string, std::string> values_;
    };
}

namespace facebook::velox::core {
    class QueryConfig {
    public:
         QueryConfig(std::unordered_map<std::string, std::string> values) {}
    };
}
