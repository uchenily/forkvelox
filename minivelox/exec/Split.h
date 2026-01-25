#pragma once

#include <memory>
#include <string>

namespace facebook::velox::connector {
class ConnectorSplit {
public:
    virtual ~ConnectorSplit() = default;
};
}

namespace facebook::velox::exec {

struct Split {
    std::shared_ptr<connector::ConnectorSplit> connectorSplit;
    int32_t groupId = -1;
    
    Split(std::shared_ptr<connector::ConnectorSplit> split, int32_t group = -1) 
        : connectorSplit(std::move(split)), groupId(group) {}
};

} // namespace facebook::velox::exec

namespace facebook::velox::connector::tpch {
class TpchConnectorSplit : public ConnectorSplit {
public:
    TpchConnectorSplit(std::string id, bool cacheable, int parts, int part) {}
};
}
