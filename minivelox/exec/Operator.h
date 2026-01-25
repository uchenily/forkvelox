#pragma once

#include "exec/PlanNode.h"
#include "vector/RowVector.h"
#include "common/memory/Memory.h"
#include "core/Context.h"

namespace facebook::velox::exec {

class DriverCtx;

class OperatorCtx {
public:
    OperatorCtx(DriverCtx* driverCtx, core::PlanNodeId planNodeId, int32_t operatorId, memory::MemoryPool* pool)
        : driverCtx_(driverCtx), planNodeId_(std::move(planNodeId)), operatorId_(operatorId), pool_(pool) {}

    memory::MemoryPool* pool() const { return pool_; }
    DriverCtx* driverCtx() const { return driverCtx_; }
    core::ExecCtx* execCtx() { return &execCtx_; }

private:
    DriverCtx* driverCtx_;
    core::PlanNodeId planNodeId_;
    int32_t operatorId_;
    memory::MemoryPool* pool_;
    // Quick hack: embed ExecCtx
    core::ExecCtx execCtx_{pool_, nullptr}; // QueryCtx null?
};

class Operator {
public:
    Operator(std::unique_ptr<OperatorCtx> ctx) : ctx_(std::move(ctx)) {}
    virtual ~Operator() = default;

    // Push input to this operator. Returns true if input was accepted (operator not blocked).
    virtual bool needsInput() const { return true; }
    virtual void addInput(RowVectorPtr input) = 0;

    // Pull output from this operator. Returns nullptr if no output available (needs more input or finished).
    virtual RowVectorPtr getOutput() = 0;

    // Check if operator is finished (will produce no more output).
    virtual bool isFinished() = 0;
    
    // Signal that no more input will be added
    virtual void noMoreInput() {}
    
    OperatorCtx* operatorCtx() const { return ctx_.get(); }
    memory::MemoryPool* pool() const { return ctx_->pool(); }

protected:
    std::unique_ptr<OperatorCtx> ctx_;
};

} // namespace facebook::velox::exec
