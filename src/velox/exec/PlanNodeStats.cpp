#include "velox/exec/PlanNodeStats.h"

#include <algorithm>
#include <iomanip>
#include <iterator>
#include <map>
#include <set>
#include <sstream>
#include <unordered_set>

namespace facebook::velox::exec {
namespace {

std::string formatBytes(uint64_t bytes) {
  static const char *kUnits[] = {"B", "KB", "MB", "GB", "TB"};
  if (bytes < 1024) {
    return std::to_string(bytes) + "B";
  }

  double value = static_cast<double>(bytes);
  size_t unit = 0;
  while (value >= 1024.0 && unit + 1 < std::size(kUnits)) {
    value /= 1024.0;
    ++unit;
  }

  std::ostringstream out;
  out << std::fixed << std::setprecision(2) << value << kUnits[unit];
  return out.str();
}

std::string formatNanos(uint64_t nanos) {
  if (nanos < 1'000) {
    return std::to_string(nanos) + "ns";
  }
  if (nanos < 1'000'000) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(2) << (static_cast<double>(nanos) / 1'000.0) << "us";
    return out.str();
  }
  if (nanos < 1'000'000'000) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(2) << (static_cast<double>(nanos) / 1'000'000.0) << "ms";
    return out.str();
  }
  std::ostringstream out;
  out << std::fixed << std::setprecision(2) << (static_cast<double>(nanos) / 1'000'000'000.0) << "s";
  return out.str();
}

void appendOperatorStats(const OperatorStats &stats, std::unordered_map<core::PlanNodeId, PlanNodeStats> &planStats) {
  auto it = planStats.find(stats.planNodeId);
  if (it == planStats.end()) {
    PlanNodeStats nodeStats;
    nodeStats.add(stats);
    planStats.emplace(stats.planNodeId, std::move(nodeStats));
    return;
  }
  it->second.add(stats);
}

void collectLeafPlanNodeIds(const core::PlanNode &plan, std::unordered_set<core::PlanNodeId> &leafPlanNodes) {
  const auto sources = plan.sources();
  if (sources.empty()) {
    leafPlanNodes.insert(plan.id());
    return;
  }
  for (const auto &source : sources) {
    if (source) {
      collectLeafPlanNodeIds(*source, leafPlanNodes);
    }
  }
}

void printPlanWithStatsRecursive(const core::PlanNode &plan,
                                 const std::unordered_map<core::PlanNodeId, PlanNodeStats> &planStats,
                                 const std::unordered_set<core::PlanNodeId> &leafPlanNodes,
                                 const std::string &indentation, std::ostream &out) {
  out << indentation << plan.toString(true, false) << "\n";

  const auto statsIt = planStats.find(plan.id());
  const PlanNodeStats *stats = statsIt == planStats.end() ? nullptr : &statsIt->second;
  const auto includeInputStats = leafPlanNodes.count(plan.id()) > 0;
  out << indentation << "   " << (stats ? stats->toString(includeInputStats) : PlanNodeStats{}.toString(includeInputStats))
      << "\n";

  if (stats && stats->isMultiOperatorTypeNode()) {
    std::map<std::string, const PlanNodeStats *> ordered;
    for (const auto &[name, opStats] : stats->operatorStats) {
      ordered.emplace(name, opStats.get());
    }

    for (const auto &[name, opStats] : ordered) {
      out << indentation << "   " << name << ": " << opStats->toString(true) << "\n";
    }
  }

  for (const auto &source : plan.sources()) {
    if (source) {
      printPlanWithStatsRecursive(*source, planStats, leafPlanNodes, indentation + "  ", out);
    }
  }
}

} // namespace

void PlanNodeStats::add(const OperatorStats &stats) {
  auto it = operatorStats.find(stats.operatorType);
  if (it == operatorStats.end()) {
    auto opStats = std::make_unique<PlanNodeStats>();
    opStats->addTotals(stats);
    operatorStats.emplace(stats.operatorType, std::move(opStats));
  } else {
    it->second->addTotals(stats);
  }
  addTotals(stats);
}

void PlanNodeStats::addTotals(const OperatorStats &stats) {
  inputRows += stats.inputRows;
  inputVectors += stats.inputVectors;
  inputBytes += stats.inputBytes;

  rawInputRows += stats.rawInputRows;
  rawInputBytes += stats.rawInputBytes;

  outputRows += stats.outputRows;
  outputVectors += stats.outputVectors;
  outputBytes += stats.outputBytes;

  addInputCpuNanos += stats.addInputCpuNanos;
  getOutputCpuNanos += stats.getOutputCpuNanos;
  finishCpuNanos += stats.finishCpuNanos;
  cpuNanos += stats.cpuNanos;

  blockedWallNanos += stats.blockedWallNanos;
  peakMemoryBytes = std::max<uint64_t>(peakMemoryBytes, stats.peakMemoryBytes);

  numDrivers += stats.numDrivers;
  numSplits += stats.numSplits;
}

std::string PlanNodeStats::toString(bool includeInputStats) const {
  std::stringstream out;

  if (includeInputStats) {
    out << "Input: " << inputRows << " rows (" << formatBytes(inputBytes) << "), ";
    if (rawInputRows > 0 && rawInputRows != inputRows) {
      out << "Raw Input: " << rawInputRows << " rows (" << formatBytes(rawInputBytes) << "), ";
    }
  }

  out << "Output: " << outputRows << " rows (" << formatBytes(outputBytes) << ")";
  out << ", Cpu time: " << formatNanos(cpuNanos);
  out << ", Blocked wall time: " << formatNanos(blockedWallNanos);
  out << ", Peak memory: " << formatBytes(peakMemoryBytes);

  if (numDrivers > 0) {
    out << ", Threads: " << numDrivers;
  }
  if (numSplits > 0) {
    out << ", Splits: " << numSplits;
  }

  out << ", CPU breakdown: I/O/F (" << formatNanos(addInputCpuNanos) << "/" << formatNanos(getOutputCpuNanos) << "/"
      << formatNanos(finishCpuNanos) << ")";

  return out.str();
}

std::unordered_map<core::PlanNodeId, PlanNodeStats> toPlanStats(const TaskStats &taskStats) {
  std::unordered_map<core::PlanNodeId, PlanNodeStats> planStats;
  for (const auto &pipeline : taskStats.pipelineStats) {
    for (const auto &opStats : pipeline.operatorStats) {
      appendOperatorStats(opStats, planStats);
    }
  }
  return planStats;
}

std::string printPlanWithStats(const core::PlanNode &plan, const TaskStats &taskStats, bool includeCustomStats) {
  (void)includeCustomStats;
  const auto planStats = toPlanStats(taskStats);
  std::unordered_set<core::PlanNodeId> leafPlanNodes;
  collectLeafPlanNodeIds(plan, leafPlanNodes);

  std::stringstream out;
  printPlanWithStatsRecursive(plan, planStats, leafPlanNodes, "", out);
  return out.str();
}

} // namespace facebook::velox::exec
