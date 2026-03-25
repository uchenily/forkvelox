#pragma once

#include <cstdint>
#include <future>

namespace facebook::velox::exec {

using ContinueFuture = std::shared_future<void>;
using ContinuePromise = std::promise<void>;

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
