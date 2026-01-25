#include "exec/Driver.h"
#include <iostream>

namespace facebook::velox::exec {

void Driver::run() {
    while (step()) {}
}

bool Driver::step() {
    // Simplified driver loop: push-based from source or pull-based from sink?
    // Velox: Drivers are typically associated with a pipeline.
    // Source operator produces, then we push through.
    // Or Sink pulls.
    
    // Let's assume the pipeline is Source -> Op -> ... -> Sink (or null sink).
    // If we have a sequence of operators, we can try to pull from the last one?
    // No, usually we pull from Source and push down.
    
    // But Operator interface I defined has `getOutput` and `addInput`.
    // Standard Volcano: pull from last.
    // Velox: Push-based. Source::getOutput -> next::addInput ...
    
    // Let's implement a simple loop that moves batches from Source to Sink.
    
    // If operators_[0] is Source:
    auto& source = operators_[0];
    if (source->isFinished()) return false;
    
    RowVectorPtr batch = source->getOutput();
    if (!batch) {
        // Source blocked or finished?
        if (source->isFinished()) return false;
        return true; // yield
    }
    
    // Push through rest
    for (size_t i = 1; i < operators_.size(); ++i) {
        operators_[i]->addInput(batch);
        // Does operator produce output immediately? 
        // Some might buffer (agg, sort).
        // Some are streaming (filter, project).
        // If streaming, getOutput immediately?
        // If blocking, getOutput returns null until ready.
        
        // This linear chain assumes 1-to-1 or 1-to-N batch mapping.
        // But if Op[i] buffers, it returns null. Then subsequent Ops get null?
        // We need a loop inside to drain Op[i] before feeding next?
        
        // Better:
        // Feed batch to Op[i].
        // While (Op[i] has output):
        //    Pass output to Op[i+1]...
        
        // This requires recursive or stack-based processing.
    }
    
    // However, the last operator might produce output we want to collect?
    // If there is no sink, where does data go?
    // In Demo, `AssertQueryBuilder` collects results. It acts as a Sink or we pull from Plan?
    // The Demo code: `AssertQueryBuilder(plan).copyResults(pool)`.
    // This implies we pull from the plan (root node).
    
    // So for the demo, we probably want to *pull* from the last operator.
    // `Driver` might not be the main entry point for `AssertQueryBuilder`.
    // `AssertQueryBuilder` likely creates a Task, Driver, and then pulls from the root operator.
    
    // So I should implement `Driver` to support `next()`?
    // Or just let `AssertQueryBuilder` own the driver and pull from root?
    
    // Let's make `Driver` capable of pulling from the last operator if requested.
    return true; 
}

} // namespace facebook::velox::exec
