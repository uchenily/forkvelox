#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include "velox/core/QueryConfig.h"

namespace facebook::velox::connector {
    class Connector {
    public:
         virtual ~Connector() = default;
    };
    inline void registerConnector(std::shared_ptr<Connector> c) {}
    inline void unregisterConnector(std::string id) {}
}

namespace facebook::velox::connector::tpch {
    class TpchConnector : public Connector {
    };
    
    class TpchConnectorFactory {
    public:
        std::shared_ptr<TpchConnector> newConnector(const std::string& id, std::shared_ptr<config::ConfigBase> config) {
            return std::make_shared<TpchConnector>();
        }
    };
}
