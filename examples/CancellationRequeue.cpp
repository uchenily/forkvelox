#include <chrono>
#include <folly/init/Init.h>
#include <iostream>
#include <thread>

#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/Driver.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

class DummyPlanNode : public core::PlanNode {
public:
  explicit DummyPlanNode(core::PlanNodeId id) : id_(std::move(id)) {}
  const core::PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "Dummy"; }
  std::vector<std::shared_ptr<const core::PlanNode>> sources() const override { return {}; }

private:
  core::PlanNodeId id_;
};

class SlowValuesOperator : public Operator {
public:
  SlowValuesOperator(core::PlanNodePtr node, std::vector<RowVectorPtr> values, std::chrono::milliseconds sleepPerBatch)
      : Operator(std::move(node)), values_(std::move(values)), sleepPerBatch_(sleepPerBatch) {}

  bool needsInput() const override { return false; }
  void addInput(RowVectorPtr) override {}

  RowVectorPtr getOutput() override {
    if (current_ >= values_.size()) {
      finished_ = true;
      return nullptr;
    }
    std::this_thread::sleep_for(sleepPerBatch_);
    return values_[current_++];
  }

  bool isFinished() override { return finished_; }

private:
  std::vector<RowVectorPtr> values_;
  size_t current_{0};
  std::chrono::milliseconds sleepPerBatch_{0};
  bool finished_{false};
};

std::vector<RowVectorPtr> makeBatches(memory::MemoryPool *pool, vector_size_t batchSize, int numBatches) {
  std::vector<RowVectorPtr> batches;
  batches.reserve(numBatches);
  auto rowType = ROW({"value"}, {BIGINT()});
  for (int batch = 0; batch < numBatches; ++batch) {
    auto buffer = AlignedBuffer::allocate(batchSize * sizeof(int64_t), pool);
    auto *rawValues = buffer->asMutable<int64_t>();
    for (vector_size_t i = 0; i < batchSize; ++i) {
      rawValues[i] = batch * batchSize + i;
    }
    auto vector = std::make_shared<FlatVector<int64_t>>(pool, BIGINT(), nullptr, batchSize, buffer);
    batches.push_back(std::make_shared<RowVector>(pool, rowType, nullptr, batchSize, std::vector<VectorPtr>{vector}));
  }
  return batches;
}

} // namespace

int main(int argc, char **argv) {
  folly::init::Init init{&argc, &argv, false};

  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  auto batches = makeBatches(pool.get(), 128, 200);
  auto planNode = std::make_shared<DummyPlanNode>("demo");
  auto slowOp = std::make_shared<SlowValuesOperator>(planNode, batches, std::chrono::milliseconds(2));

  Driver driver({slowOp});

  std::atomic<bool> cancel{false};
  std::thread cancelThread([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    cancel.store(true, std::memory_order_relaxed);
  });

  driver.setCancelCheck([&]() { return cancel.load(std::memory_order_relaxed); });

  std::vector<RowVectorPtr> results;
  int yields = 0;
  while (true) {
    auto start = std::chrono::steady_clock::now();
    driver.setYieldCheck([start]() { return std::chrono::steady_clock::now() - start > std::chrono::milliseconds(3); });

    auto reason = driver.run(results);
    if (reason == BlockingReason::kYield) {
      ++yields;
      std::cout << "[Demo] Requeue after yield (" << yields << ")." << std::endl;
      continue;
    }
    if (reason == BlockingReason::kCancelled) {
      std::cout << "[Demo] Cancelled after " << results.size() << " batches." << std::endl;
      break;
    }
    if (reason == BlockingReason::kNotBlocked) {
      std::cout << "[Demo] Completed with " << results.size() << " batches." << std::endl;
      break;
    }
    std::cout << "[Demo] Stopped with reason " << static_cast<int>(reason) << "." << std::endl;
    break;
  }

  cancelThread.join();
  return 0;
}
