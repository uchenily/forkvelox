#pragma once

#include "type/Type.h"
#include <vector>
#include <memory>
#include <string>

namespace facebook::velox::core {

using PlanNodeId = std::string;

class PlanNode {
public:
    PlanNode(PlanNodeId id) : id_(std::move(id)) {}
    virtual ~PlanNode() = default;

    const PlanNodeId& id() const { return id_; }
    
    virtual const RowTypePtr& outputType() const = 0;
    virtual std::vector<std::shared_ptr<PlanNode>> sources() const = 0;
    virtual std::string toString() const = 0;

protected:
    PlanNodeId id_;
};

using PlanNodePtr = std::shared_ptr<PlanNode>;

class PlanNodeIdGenerator {
public:
    std::string next() {
        return std::to_string(nextId_++);
    }
private:
    int nextId_ = 0;
};

} // namespace facebook::velox::core
