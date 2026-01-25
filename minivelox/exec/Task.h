#pragma once

#include "exec/Driver.h"
#include "common/memory/Memory.h"
#include "exec/PlanNode.h"
#include "core/Context.h"
#include <functional>

namespace facebook::velox::exec {

class Task {
public:
    Task(const std::string& taskId, core::PlanNodePtr planNode, int destination, 
         std::shared_ptr<core::QueryCtx> queryCtx, 
         std::function<void(std::exception_ptr)> onError = nullptr) 
         : taskId_(taskId), planNode_(std::move(planNode)), queryCtx_(std::move(queryCtx)) {}
         
    // For demo, we just need to start/run.
    void start(int maxDrivers = 1, int concurrentSplitGroups = 1);
    
    // Helpers
    memory::MemoryPool* pool() const { return queryCtx_->pool(); } // Simplified: assume QueryCtx has pool or we use global

private:
    std::string taskId_;
    core::PlanNodePtr planNode_;
    std::shared_ptr<core::QueryCtx> queryCtx_;
};

} // namespace facebook::velox::exec
