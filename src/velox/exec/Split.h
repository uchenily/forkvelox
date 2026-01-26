#pragma once

#include <memory>
#include <string>

#include "velox/connectors/tpch/TpchConnectorSplit.h"

namespace facebook::velox::exec {

class Split {
public:
  explicit Split(std::string path) : path_(std::move(path)) {}
  explicit Split(std::shared_ptr<connector::tpch::TpchConnectorSplit> /*split*/) {}

  const std::string& path() const { return path_; }

private:
  std::string path_;
};

} // namespace facebook::velox::exec
