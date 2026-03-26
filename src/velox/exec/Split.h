#pragma once

#include <memory>
#include <string>

#include "velox/connectors/tpch/TpchConnectorSplit.h"

namespace facebook::velox::exec {

class Split {
public:
  explicit Split(std::string path) : path_(std::move(path)) {}
  explicit Split(std::shared_ptr<connector::tpch::TpchConnectorSplit> split) : tpchSplit_(std::move(split)) {}

  const std::string &path() const { return path_; }
  const std::shared_ptr<connector::tpch::TpchConnectorSplit>& tpchSplit() const { return tpchSplit_; }
  bool isTpchSplit() const { return tpchSplit_ != nullptr; }

private:
  std::string path_;
  std::shared_ptr<connector::tpch::TpchConnectorSplit> tpchSplit_;
};

} // namespace facebook::velox::exec
