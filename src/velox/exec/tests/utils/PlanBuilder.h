#pragma once
#include "velox/common/Tree.h"
#include "velox/core/PlanNode.h"
#include "velox/core/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/tpch/gen/TpchGen.h"
#include <iostream>
#include <memory>
#include <string_view>

namespace facebook::velox::exec::test {

class PlanBuilder {
public:
    PlanBuilder(std::shared_ptr<core::PlanNodeIdGenerator> generator = std::make_shared<core::PlanNodeIdGenerator>()) 
        : generator_(generator) {}

    PlanBuilder& values(const std::vector<RowVectorPtr>& values) {
        root_ = std::make_shared<core::ValuesNode>(generator_->next(), values);
        return *this;
    }

    PlanBuilder& singleAggregation(
        const std::vector<std::string>& groupingKeys,
        const std::vector<std::string>& aggregates,
        core::AggregationNode::Step step = core::AggregationNode::Step::kSingle) {
        root_ = std::make_shared<core::AggregationNode>(
            generator_->next(), root_, groupingKeys, aggregates, step);
        return *this;
    }

    PlanBuilder& partialAggregation(
        const std::vector<std::string>& groupingKeys,
        const std::vector<std::string>& aggregates) {
        return singleAggregation(
            groupingKeys, aggregates, core::AggregationNode::Step::kPartial);
    }

    PlanBuilder& intermediateAggregation(
        const std::vector<std::string>& groupingKeys,
        const std::vector<std::string>& aggregates) {
        return singleAggregation(
            groupingKeys,
            aggregates,
            core::AggregationNode::Step::kIntermediate);
    }

    PlanBuilder& finalAggregation(
        const std::vector<std::string>& groupingKeys,
        const std::vector<std::string>& aggregates) {
        return singleAggregation(
            groupingKeys, aggregates, core::AggregationNode::Step::kFinal);
    }

    PlanBuilder& orderBy(const std::vector<std::string>& keys, bool isPartial) {
        root_ = std::make_shared<core::OrderByNode>(
            generator_->next(), root_, keys, isPartial);
        return *this;
    }

    PlanBuilder& tableWrite(const std::string& path) {
        root_ = std::make_shared<core::TableWriteNode>(generator_->next(), root_, path);
        return *this;
    }
    
    PlanBuilder& topN(const std::vector<std::string>& keys, int count, bool isPartial) {
        root_ = std::make_shared<core::TopNNode>(generator_->next(), root_, count, keys);
        return *this;
    }

    PlanBuilder& filter(const std::string& expr) {
        parse::DuckSqlExpressionsParser parser;
        auto typedExpr = parser.parseExpr(expr);
        root_ = std::make_shared<core::FilterNode>(generator_->next(), root_, typedExpr);
        return *this;
    }

    PlanBuilder& tableScan(const RowTypePtr& rowType, const std::string& path) {
        root_ = std::make_shared<core::FileScanNode>(generator_->next(), rowType, path);
        return *this;
    }

    PlanBuilder& localPartition(const std::string& exchangeId) {
        root_ = std::make_shared<core::LocalPartitionNode>(
            generator_->next(), root_, exchangeId);
        return *this;
    }

    PlanBuilder& localMerge(const std::string& exchangeId) {
        root_ = std::make_shared<core::LocalMergeNode>(
            generator_->next(), root_, exchangeId);
        return *this;
    }

    // Stub for TPCH
    PlanBuilder& tpchTableScan(
                 tpch::Table table,
                 std::vector<std::string> columns,
                 int scaleFactor) {
        root_ = std::make_shared<core::TableScanNode>(generator_->next(), table, columns);
        return *this;
    }
    
    PlanBuilder& capturePlanNodeId(core::PlanNodeId& id) {
        if (root_) id = root_->id();
        return *this;
    }
    
    PlanBuilder& hashJoin(
                 const std::vector<std::string>& leftKeys,
                 const std::vector<std::string>& rightKeys,
                 core::PlanNodePtr buildSide,
                 const std::string& filter,
                 const std::vector<std::string>& output) {
         root_ = std::make_shared<core::HashJoinNode>(generator_->next(), root_, buildSide);
         return *this;
    }

    std::string planTree(std::string_view title = "") const {
        if (!root_) {
            auto empty = std::make_shared<common::Tree>("(empty plan)");
            return common::DrawableTree::fromTree(empty)->render(title);
        }
        auto tree = buildPlanTree(root_);
        return common::DrawableTree::fromTree(tree)->render(title);
    }

    void printPlanTree(std::string_view title = "") const {
        std::cout << planTree(title);
    }

    core::PlanNodePtr planNode() {
        return root_;
    }

private:
    static std::shared_ptr<common::Tree> buildPlanTree(
        const core::PlanNodePtr& node) {
        auto label = node->toString() + "(" + node->id() + ")";
        auto tree = std::make_shared<common::Tree>(std::move(label));
        for (const auto& source : node->sources()) {
            tree->children.push_back(buildPlanTree(source));
        }
        return tree;
    }

    std::shared_ptr<core::PlanNodeIdGenerator> generator_;
    core::PlanNodePtr root_;
};

}
