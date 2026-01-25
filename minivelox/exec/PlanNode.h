#pragma once

#include "type/Type.h"
#include "exec/Expr.h"
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

// ValuesNode
class ValuesNode : public PlanNode {
public:
    ValuesNode(PlanNodeId id, std::vector<RowVectorPtr> values) 
        : PlanNode(std::move(id)), values_(std::move(values)) {
        if (!values_.empty()) {
            outputType_ = std::dynamic_pointer_cast<const RowType>(values_[0]->type());
        } else {
             outputType_ = std::dynamic_pointer_cast<const RowType>(RowType::create({}, {}));
        }
    }

    const RowTypePtr& outputType() const override { return outputType_; }
    std::vector<PlanNodePtr> sources() const override { return {}; }
    std::string toString() const override { return "Values"; }
    
    const std::vector<RowVectorPtr>& values() const { return values_; }

private:
    std::vector<RowVectorPtr> values_;
    RowTypePtr outputType_;
};

// FilterNode
class FilterNode : public PlanNode {
public:
    FilterNode(PlanNodeId id, PlanNodePtr source, exec::ExprPtr filter)
        : PlanNode(std::move(id)), source_(std::move(source)), filter_(std::move(filter)) {}

    const RowTypePtr& outputType() const override { return source_->outputType(); }
    std::vector<PlanNodePtr> sources() const override { return {source_}; }
    std::string toString() const override { return "Filter(" + filter_->toString() + ")"; }
    
    const exec::ExprPtr& filter() const { return filter_; }

private:
    PlanNodePtr source_;
    exec::ExprPtr filter_;
};

// ProjectNode
class ProjectNode : public PlanNode {
public:
    ProjectNode(PlanNodeId id, PlanNodePtr source, std::vector<std::string> names, std::vector<exec::ExprPtr> projections)
        : PlanNode(std::move(id)), source_(std::move(source)), names_(std::move(names)), projections_(std::move(projections)) {
        std::vector<TypePtr> types;
        for (auto& p : projections_) types.push_back(p->type());
        outputType_ = std::dynamic_pointer_cast<const RowType>(RowType::create(names_, types));
    }
    
    const RowTypePtr& outputType() const override { return outputType_; }
    std::vector<PlanNodePtr> sources() const override { return {source_}; }
    std::string toString() const override { return "Project"; }
    
    const std::vector<exec::ExprPtr>& projections() const { return projections_; }
    const std::vector<std::string>& names() const { return names_; }

private:
    PlanNodePtr source_;
    std::vector<std::string> names_;
    std::vector<exec::ExprPtr> projections_;
    RowTypePtr outputType_;
};

// AggregationNode
class AggregationNode : public PlanNode {
public:
    AggregationNode(PlanNodeId id, PlanNodePtr source, std::vector<std::string> groupingKeys, 
                    std::vector<std::string> aggregateNames, std::vector<exec::ExprPtr> aggregates)
        : PlanNode(std::move(id)), source_(std::move(source)), groupingKeys_(std::move(groupingKeys)),
          aggregateNames_(std::move(aggregateNames)), aggregates_(std::move(aggregates)) {
         
         std::vector<std::string> names = groupingKeys_;
         std::vector<TypePtr> types;
         
         auto srcRowType = source_->outputType();
         for (const auto& key : groupingKeys_) {
             bool found = false;
             for (size_t i = 0; i < srcRowType->size(); ++i) {
                 if (srcRowType->nameOf(i) == key) {
                     types.push_back(srcRowType->childAt(i));
                     found = true;
                     break;
                 }
             }
             if (!found) throw std::runtime_error("Grouping key not found: " + key);
         }
         
         for (size_t i = 0; i < aggregates_.size(); ++i) {
             names.push_back(aggregateNames_[i]);
             types.push_back(aggregates_[i]->type());
         }
         
         outputType_ = std::dynamic_pointer_cast<const RowType>(RowType::create(names, types));
    }

    const RowTypePtr& outputType() const override { return outputType_; }
    std::vector<PlanNodePtr> sources() const override { return {source_}; }
    std::string toString() const override { return "Aggregation"; }
    
    const std::vector<std::string>& groupingKeys() const { return groupingKeys_; }
    const std::vector<exec::ExprPtr>& aggregates() const { return aggregates_; }

private:
    PlanNodePtr source_;
    std::vector<std::string> groupingKeys_;
    std::vector<std::string> aggregateNames_;
    std::vector<exec::ExprPtr> aggregates_; 
    RowTypePtr outputType_;
};

// TableScanNode
class TableScanNode : public PlanNode {
public:
    TableScanNode(PlanNodeId id, RowTypePtr outputType) 
        : PlanNode(std::move(id)), outputType_(std::move(outputType)) {}
        
    const RowTypePtr& outputType() const override { return outputType_; }
    std::vector<PlanNodePtr> sources() const override { return {}; }
    std::string toString() const override { return "TableScan"; }
private:
    RowTypePtr outputType_;
};

// OrderByNode
class OrderByNode : public PlanNode {
public:
    OrderByNode(PlanNodeId id, PlanNodePtr source, std::vector<std::string> sortingKeys, 
                std::vector<bool> sortingOrders, bool isPartial)
        : PlanNode(std::move(id)), source_(std::move(source)), sortingKeys_(std::move(sortingKeys)),
          sortingOrders_(std::move(sortingOrders)), isPartial_(isPartial) {}

    const RowTypePtr& outputType() const override { return source_->outputType(); }
    std::vector<PlanNodePtr> sources() const override { return {source_}; }
    std::string toString() const override { return "OrderBy"; }
    
    const std::vector<std::string>& sortingKeys() const { return sortingKeys_; }
    const std::vector<bool>& sortingOrders() const { return sortingOrders_; } // true = asc?

private:
    PlanNodePtr source_;
    std::vector<std::string> sortingKeys_;
    std::vector<bool> sortingOrders_;
    bool isPartial_;
};

// TopNNode
class TopNNode : public PlanNode {
public:
    TopNNode(PlanNodeId id, PlanNodePtr source, int32_t count, std::vector<std::string> sortingKeys, 
             std::vector<bool> sortingOrders, bool isPartial)
        : PlanNode(std::move(id)), source_(std::move(source)), count_(count), 
          sortingKeys_(std::move(sortingKeys)), sortingOrders_(std::move(sortingOrders)), isPartial_(isPartial) {}

    const RowTypePtr& outputType() const override { return source_->outputType(); }
    std::vector<PlanNodePtr> sources() const override { return {source_}; }
    std::string toString() const override { return "TopN"; }
    
    int32_t count() const { return count_; }
    const std::vector<std::string>& sortingKeys() const { return sortingKeys_; }
    const std::vector<bool>& sortingOrders() const { return sortingOrders_; }

private:
    PlanNodePtr source_;
    int32_t count_;
    std::vector<std::string> sortingKeys_;
    std::vector<bool> sortingOrders_;
    bool isPartial_;
};

// HashJoinNode
class HashJoinNode : public PlanNode {
public:
    HashJoinNode(PlanNodeId id, PlanNodePtr left, PlanNodePtr right, 
                 std::vector<std::string> leftKeys, std::vector<std::string> rightKeys,
                 std::vector<std::string> outputColumns)
        : PlanNode(std::move(id)), left_(std::move(left)), right_(std::move(right)),
          leftKeys_(std::move(leftKeys)), rightKeys_(std::move(rightKeys)), outputColumns_(std::move(outputColumns)) {
          
          // Compute output type based on outputColumns
          // Need to find type of each output column from left or right
          std::vector<std::string> names = outputColumns_;
          std::vector<TypePtr> types;
          
          auto leftType = left_->outputType();
          auto rightType = right_->outputType();
          
          for (const auto& col : outputColumns_) {
              bool found = false;
              for(size_t i=0; i<leftType->size(); ++i) {
                  if (leftType->nameOf(i) == col) {
                      types.push_back(leftType->childAt(i));
                      found = true; break;
                  }
              }
              if (!found) {
                  for(size_t i=0; i<rightType->size(); ++i) {
                      if (rightType->nameOf(i) == col) {
                          types.push_back(rightType->childAt(i));
                          found = true; break;
                      }
                  }
              }
              if (!found) throw std::runtime_error("Column not found in join inputs: " + col);
          }
          
          outputType_ = std::dynamic_pointer_cast<const RowType>(RowType::create(names, types));
    }
    
    const RowTypePtr& outputType() const override { return outputType_; }
    std::vector<PlanNodePtr> sources() const override { return {left_, right_}; }
    std::string toString() const override { return "HashJoin"; }
    
    const PlanNodePtr& left() const { return left_; }
    const PlanNodePtr& right() const { return right_; }
    const std::vector<std::string>& leftKeys() const { return leftKeys_; }
    const std::vector<std::string>& rightKeys() const { return rightKeys_; }

private:
    PlanNodePtr left_;
    PlanNodePtr right_;
    std::vector<std::string> leftKeys_;
    std::vector<std::string> rightKeys_;
    std::vector<std::string> outputColumns_;
    RowTypePtr outputType_;
};

} // namespace facebook::velox::core
