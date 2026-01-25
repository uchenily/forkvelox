#pragma once
#include <memory>
#include "velox/connectors/tpch/TpchConnectorSplit.h"
namespace facebook::velox::exec {
    class Split {
    public:
        Split(std::shared_ptr<connector::tpch::TpchConnectorSplit> split) {}
    };
}
