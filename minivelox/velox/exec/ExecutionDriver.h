#pragma once
#include "velox/exec/Operator.h"
#include <vector>
#include <memory>
#include <iostream>

namespace facebook::velox::exec {

class ExecutionDriver {
public:
    ExecutionDriver(std::vector<std::shared_ptr<Operator>> operators) : operators_(std::move(operators)) {}
    
    std::vector<RowVectorPtr> run() {
        std::cout << "[Driver] Starting execution pipeline." << std::endl;
        std::vector<RowVectorPtr> results;
        bool progress = true;
        while (progress) {
            progress = false;
            
            auto& source = operators_.front();
            std::cout << "[Driver] Pulling from source " << source->planNode()->toString() << std::endl;
            auto batch = source->getOutput();
            if (batch) {
                std::cout << "[Driver] Source produced " << batch->size() << std::endl;
                progress = true;
                for (size_t i = 1; i < operators_.size(); ++i) {
                    operators_[i]->addInput(batch);
                    batch = operators_[i]->getOutput();
                    if (!batch) break;
                }
                if (batch) {
                    results.push_back(batch);
                }
            } else {
                 if (!source->isFinished()) {
                 } else {
                     std::cout << "[Driver] Source finished." << std::endl;
                     // Propagate finish
                     for (size_t i = 1; i < operators_.size(); ++i) {
                         operators_[i]->noMoreInput();
                     }

                     
                     // Now flush pipeline
                     progress = true; // Retry loop to flush buffers
                     while(progress) {
                         progress = false;
                         for (size_t i = 1; i < operators_.size(); ++i) {
                             auto batch = operators_[i]->getOutput();
                             if (batch) {
                                 std::cout << "[Driver] Flushed batch from " << operators_[i]->planNode()->toString() << std::endl;
                                 if (i == operators_.size() - 1) results.push_back(batch);
                                 else operators_[i+1]->addInput(batch);
                                 progress = true;
                             }
                         }
                     }
                     break; // Done
                 }
            }
        }
        std::cout << "[Driver] Execution finished." << std::endl;
        return results;
    }
    
private:
    std::vector<std::shared_ptr<Operator>> operators_;
};

}
