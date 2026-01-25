#pragma once
#include <memory>
#include <string>
#include <vector>
#include "velox/vector/ComplexVector.h"
#include "velox/core/ITypedExpr.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::velox::core {

using PlanNodeId = std::string;

class PlanNode {
public:
    virtual ~PlanNode() = default;
    virtual const PlanNodeId& id() const = 0;
    virtual std::string toString() const = 0;
    virtual std::vector<std::shared_ptr<const PlanNode>> sources() const = 0;
};

using PlanNodePtr = std::shared_ptr<const PlanNode>;

class PlanNodeIdGenerator {
public:
    std::string next() { return std::to_string(nextId_++); }
private:
    int nextId_ = 0;
};

// Nodes
class ValuesNode : public PlanNode {
public:
    ValuesNode(PlanNodeId id, std::vector<RowVectorPtr> values) : id_(id), values_(std::move(values)) {}
    const PlanNodeId& id() const override { return id_; }
    std::string toString() const override { return "Values"; }
    std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {}; }
    const std::vector<RowVectorPtr>& values() const { return values_; }
private:
    PlanNodeId id_;
    std::vector<RowVectorPtr> values_;
};

class FilterNode : public PlanNode {
public:
    FilterNode(PlanNodeId id, PlanNodePtr source, TypedExprPtr filter) 
        : id_(id), source_(source), filter_(filter) {}
    const PlanNodeId& id() const override { return id_; }
    std::string toString() const override { return "Filter"; }
    std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {source_}; }
    const TypedExprPtr& filter() const { return filter_; }
private:
    PlanNodeId id_;
    PlanNodePtr source_;
    TypedExprPtr filter_;
};

// ... Aggregation, OrderBy, etc.
// For now I stub them to compile demo.

class AggregationNode : public PlanNode {
public:
    AggregationNode(PlanNodeId id, PlanNodePtr source, std::vector<std::string> groupingKeys, std::vector<std::string> aggregates)
        : id_(id), source_(source), groupingKeys_(std::move(groupingKeys)), aggregates_(std::move(aggregates)) {}
    const PlanNodeId& id() const override { return id_; }
    std::string toString() const override { return "Aggregation"; }
    std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {source_}; }
    const std::vector<std::string>& aggregates() const { return aggregates_; }
    const std::vector<std::string>& groupingKeys() const { return groupingKeys_; }
private:
    PlanNodeId id_;
    PlanNodePtr source_;
    std::vector<std::string> groupingKeys_;
    std::vector<std::string> aggregates_;
};

class OrderByNode : public PlanNode {
public:
    OrderByNode(PlanNodeId id, PlanNodePtr source, std::vector<std::string> keys)
        : id_(id), source_(source), keys_(std::move(keys)) {}
    const PlanNodeId& id() const override { return id_; }
    std::string toString() const override { return "OrderBy"; }
    std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {source_}; }
    const std::vector<std::string>& keys() const { return keys_; }
private:
    PlanNodeId id_;
    PlanNodePtr source_;
    std::vector<std::string> keys_;
};

class TopNNode : public PlanNode {
public:
    TopNNode(PlanNodeId id, PlanNodePtr source, int count, std::vector<std::string> keys)
        : id_(id), source_(source), count_(count), keys_(std::move(keys)) {}
    const PlanNodeId& id() const override { return id_; }
    std::string toString() const override { return "TopN"; }
    std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {source_}; }
    int count() const { return count_; }
    const std::vector<std::string>& keys() const { return keys_; }
private:
    PlanNodeId id_;
    PlanNodePtr source_;
    int count_;
    std::vector<std::string> keys_;
};

class TableScanNode : public PlanNode {
public:
    TableScanNode(PlanNodeId id, tpch::Table table, std::vector<std::string> columns) 
        : id_(id), table_(table), columns_(std::move(columns)) {}
    const PlanNodeId& id() const override { return id_; }
    std::string toString() const override { return "TableScan"; }
    std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {}; }
    tpch::Table table() const { return table_; }
    const std::vector<std::string>& columns() const { return columns_; }
private:
    PlanNodeId id_;
    tpch::Table table_;
    std::vector<std::string> columns_;
};

class HashJoinNode : public PlanNode {
public:
    HashJoinNode(PlanNodeId id, PlanNodePtr probe, PlanNodePtr build) 
        : id_(id), probe_(probe), build_(build) {}
    const PlanNodeId& id() const override { return id_; }
    std::string toString() const override { return "HashJoin"; }
    std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {probe_, build_}; }
private:
    PlanNodeId id_;
    PlanNodePtr probe_;
    PlanNodePtr build_;
};

}
