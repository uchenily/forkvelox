#pragma once

#include <cstdint>

namespace facebook::velox::exec {

enum class BlockingReason : uint8_t {
  kNotBlocked = 0,
  kWaitForSplit,
  kWaitForProducer,
  kWaitForJoinBuild,
  kYield,
  kCancelled,
};

const char* blockingReasonName(BlockingReason reason);

} // namespace facebook::velox::exec
