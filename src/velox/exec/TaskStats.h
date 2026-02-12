#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "velox/core/PlanNode.h"

namespace facebook::velox::exec {

struct OperatorStats {
  core::PlanNodeId planNodeId;
  std::string operatorType;

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

  bool touched{false};

  void add(const OperatorStats &other) {
    inputRows += other.inputRows;
    inputVectors += other.inputVectors;
    inputBytes += other.inputBytes;

    rawInputRows += other.rawInputRows;
    rawInputBytes += other.rawInputBytes;

    outputRows += other.outputRows;
    outputVectors += other.outputVectors;
    outputBytes += other.outputBytes;

    addInputCpuNanos += other.addInputCpuNanos;
    getOutputCpuNanos += other.getOutputCpuNanos;
    finishCpuNanos += other.finishCpuNanos;
    cpuNanos += other.cpuNanos;

    blockedWallNanos += other.blockedWallNanos;
    peakMemoryBytes = std::max<uint64_t>(peakMemoryBytes, other.peakMemoryBytes);

    numDrivers += other.numDrivers;
    numSplits += other.numSplits;
    touched = touched || other.touched;
  }
};

struct PipelineStats {
  std::vector<OperatorStats> operatorStats;
  bool inputPipeline{false};
  bool outputPipeline{false};

  PipelineStats() = default;
  PipelineStats(bool input, bool output) : inputPipeline(input), outputPipeline(output) {}
};

struct TaskStats {
  std::vector<PipelineStats> pipelineStats;
};

} // namespace facebook::velox::exec
