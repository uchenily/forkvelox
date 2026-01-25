#pragma once

#include "exec/Operator.h"
#include <vector>
#include <memory>

namespace facebook::velox::exec {

class Task;

class DriverCtx {
public:
    DriverCtx(std::shared_ptr<Task> t, int driverId) : task(t), driverId(driverId) {}
    std::shared_ptr<Task> task;
    int driverId;
    Operator* driver = nullptr; // Not used much in mini
};

class Driver {
public:
    Driver(std::unique_ptr<DriverCtx> ctx, std::vector<std::unique_ptr<Operator>> operators)
        : ctx_(std::move(ctx)), operators_(std::move(operators)) {}

    // Run the driver until finished.
    // In real Velox, this yields. Here we block.
    void run();
    
    // For manual stepping if needed
    bool step();

private:
    std::unique_ptr<DriverCtx> ctx_;
    std::vector<std::unique_ptr<Operator>> operators_;
};

} // namespace facebook::velox::exec
