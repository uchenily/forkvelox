#pragma once
#include "velox/tpch/gen/TpchGen.h"

#include <string>

namespace facebook::velox::connector::tpch {
class TpchConnectorSplit {
public:
  TpchConnectorSplit(
      std::string id,
      bool cacheable,
      ::facebook::velox::tpch::Table table,
      int scale,
      int part,
      int totalParts = 1)
      : id_(std::move(id)), cacheable_(cacheable), table_(table), scale_(scale), part_(part), totalParts_(totalParts) {}

  const std::string& id() const { return id_; }
  bool cacheable() const { return cacheable_; }
  ::facebook::velox::tpch::Table table() const { return table_; }
  int scale() const { return scale_; }
  int part() const { return part_; }
  int totalParts() const { return totalParts_; }

private:
  std::string id_;
  bool cacheable_;
  ::facebook::velox::tpch::Table table_;
  int scale_;
  int part_;
  int totalParts_;
};
} // namespace facebook::velox::connector::tpch
