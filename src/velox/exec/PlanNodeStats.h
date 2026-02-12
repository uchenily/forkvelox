#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "velox/core/PlanNode.h"
#include "velox/exec/TaskStats.h"

namespace facebook::velox::exec {

struct PlanNodeStats {
  uint64_t inputRows{0};
  uint64_t inputVectors{0};
  uint64_t inputBytes{0};

  uint64_t rawInputRows{0};
  uint64_t rawInputBytes{0};

  uint64_t outputRows{0};
  uint64_t outputVectors{0};
  uint64_t outputBytes{0};

  uint64_t addInputCpuNanos{0};
  uint64_t getOutputCpuNanos{0};
  uint64_t finishCpuNanos{0};
  uint64_t cpuNanos{0};

  uint64_t blockedWallNanos{0};
  uint64_t peakMemoryBytes{0};

  int numDrivers{0};
  int numSplits{0};

  std::unordered_map<std::string, std::unique_ptr<PlanNodeStats>> operatorStats;

  PlanNodeStats() = default;
  PlanNodeStats(const PlanNodeStats &) = delete;
  PlanNodeStats &operator=(const PlanNodeStats &) = delete;
  PlanNodeStats(PlanNodeStats &&) = default;
  PlanNodeStats &operator=(PlanNodeStats &&) = default;

  void add(const OperatorStats &stats);

  std::string toString(bool includeInputStats = false) const;

  bool isMultiOperatorTypeNode() const { return operatorStats.size() > 1; }

private:
  void addTotals(const OperatorStats &stats);
};

std::unordered_map<core::PlanNodeId, PlanNodeStats> toPlanStats(const TaskStats &taskStats);

std::string printPlanWithStats(const core::PlanNode &plan, const TaskStats &taskStats,
                               bool includeCustomStats = false);

} // namespace facebook::velox::exec
