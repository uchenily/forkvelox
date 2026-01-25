#pragma once
#include "velox/exec/Operator.h"
#include <deque>

namespace facebook::velox::exec {

class Driver {
public:
    Driver(std::vector<std::shared_ptr<Operator>> operators) : operators_(std::move(operators)) {}
    
    std::vector<RowVectorPtr> run() {
        std::vector<RowVectorPtr> results;
        // Simple pull model
        bool progress = true;
        while (progress) {
            progress = false;
            // Drive the pipeline
            // Root (last operator) pulls
            // But we need to push from Source.
            
            // Simplified execution:
            // 1. Source produces batch.
            // 2. Pass through pipeline.
            // 3. Collect result.
            
            auto& source = operators_.front();
            auto batch = source->getOutput();
            if (batch) {
                progress = true;
                for (size_t i = 1; i < operators_.size(); ++i) {
                    operators_[i]->addInput(batch);
                    batch = operators_[i]->getOutput();
                    if (!batch) break; // filtered out?
                }
                if (batch) results.push_back(batch);
            } else {
                 if (!source->isFinished()) {
                     // blocked?
                 } else {
                     // finished
                 }
            }
        }
        return results;
    }
    
private:
    std::vector<std::shared_ptr<Operator>> operators_;
};

}
