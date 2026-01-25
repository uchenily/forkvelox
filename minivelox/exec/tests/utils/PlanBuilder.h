#pragma once

#include "exec/PlanNode.h"
#include "parse/Expressions.h"
#include "common/memory/Memory.h"
#include "vector/RowVector.h"
#include "exec/Expr.h"

namespace facebook::velox::exec::test {

using namespace facebook::velox::core;

class PlanBuilder {
public:
    PlanBuilder(std::shared_ptr<PlanNodeIdGenerator> idGenerator = nullptr, memory::MemoryPool* pool = nullptr) 
        : idGenerator_(idGenerator ? idGenerator : std::make_shared<PlanNodeIdGenerator>()), pool_(pool) {
        if (!pool_) pool_ = memory::MemoryManager::getInstance().addRootPool("plan_builder");
    }

    PlanBuilder& values(const std::vector<RowVectorPtr>& values) {
        auto id = idGenerator_->next();
        planNode_ = std::make_shared<ValuesNode>(id, values);
        return *this;
    }
    
    PlanBuilder& filter(const std::string& expr) {
        auto id = idGenerator_->next();
        auto typedExpr = parseExpr(expr, planNode_->outputType());
        planNode_ = std::make_shared<FilterNode>(id, planNode_, typedExpr);
        return *this;
    }
    
    PlanBuilder& singleAggregation(
        const std::vector<std::string>& groupingKeys,
        const std::vector<std::string>& aggregates) {
        
        auto id = idGenerator_->next();
        
        // Parse aggregates: "sum(a) AS sum_a" -> name="sum_a", expr="sum(a)"
        std::vector<std::string> aggNames;
        std::vector<exec::ExprPtr> aggExprs;
        
        for (const auto& agg : aggregates) {
             // Split by " AS "
             size_t asPos = agg.find(" AS ");
             std::string exprStr, name;
             if (asPos != std::string::npos) {
                 exprStr = agg.substr(0, asPos);
                 name = agg.substr(asPos + 4);
             } else {
                 exprStr = agg;
                 name = "agg_" + std::to_string(aggExprs.size());
             }
             
             aggNames.push_back(name);
             aggExprs.push_back(parseExpr(exprStr, planNode_->outputType()));
        }
        
        planNode_ = std::make_shared<AggregationNode>(id, planNode_, groupingKeys, aggNames, aggExprs);
        return *this;
    }
    
    PlanBuilder& orderBy(const std::vector<std::string>& keys, bool isPartial) {
        auto id = idGenerator_->next();
        
        std::vector<std::string> sortingKeys;
        std::vector<bool> sortingOrders; // true for ASC
        
        for(const auto& k : keys) {
            // "a DESC"
            size_t descPos = k.find(" DESC");
            if (descPos != std::string::npos) {
                sortingKeys.push_back(k.substr(0, descPos));
                sortingOrders.push_back(false);
            } else {
                sortingKeys.push_back(k);
                sortingOrders.push_back(true);
            }
        }
        
        planNode_ = std::make_shared<OrderByNode>(id, planNode_, sortingKeys, sortingOrders, isPartial);
        return *this;
    }
    
    PlanBuilder& topN(const std::vector<std::string>& keys, int32_t count, bool isPartial) {
        auto id = idGenerator_->next();
        // Similar parsing as orderBy
         std::vector<std::string> sortingKeys;
        std::vector<bool> sortingOrders; 
        for(const auto& k : keys) {
            size_t descPos = k.find(" DESC");
            if (descPos != std::string::npos) {
                sortingKeys.push_back(k.substr(0, descPos));
                sortingOrders.push_back(false);
            } else {
                sortingKeys.push_back(k);
                sortingOrders.push_back(true);
            }
        }
        planNode_ = std::make_shared<TopNNode>(id, planNode_, count, sortingKeys, sortingOrders, isPartial);
        return *this;
    }
    
    // Simplified tpchTableScan
    PlanBuilder& tpchTableScan(
        int tableId,
        std::vector<std::string> columns,
        double scaleFactor) {
        
        auto id = idGenerator_->next();
        // Need to construct RowType for columns.
        // We don't have metadata catalog. 
        // Demo uses TPC-H. I need to know types of TPC-H columns.
        // n_nationkey: BIGINT, n_name: VARCHAR, n_regionkey: BIGINT, r_regionkey: BIGINT, r_name: VARCHAR
        
        std::vector<std::string> names = columns;
        std::vector<TypePtr> types;
        
        for(const auto& col : columns) {
            if (col.find("key") != std::string::npos) types.push_back(BigIntType::create());
            else if (col.find("name") != std::string::npos) types.push_back(VarcharType::create());
            else types.push_back(BigIntType::create()); // Default
        }
        
        auto type = RowType::create(names, types);
        // Cast to RowTypePtr
        auto rowType = std::dynamic_pointer_cast<const RowType>(type);
        
        planNode_ = std::make_shared<TableScanNode>(id, rowType);
        return *this;
    }
    
    PlanBuilder& hashJoin(
        const std::vector<std::string>& leftKeys,
        const std::vector<std::string>& rightKeys,
        PlanNodePtr right,
        const std::string& filter,
        const std::vector<std::string>& outputColumns) {
        
        auto id = idGenerator_->next();
        planNode_ = std::make_shared<HashJoinNode>(id, planNode_, right, leftKeys, rightKeys, outputColumns);
        return *this;
    }
    
    PlanBuilder& capturePlanNodeId(PlanNodeId& id) {
        id = planNode_->id();
        return *this;
    }

    PlanNodePtr planNode() { return planNode_; }

private:
    std::shared_ptr<PlanNodeIdGenerator> idGenerator_;
    memory::MemoryPool* pool_;
    PlanNodePtr planNode_;
    
    exec::ExprPtr parseExpr(const std::string& text, const RowTypePtr& rowType) {
        auto untyped = parse::DuckSqlExpressionsParser().parseExpr(text);
        return core::Expressions::inferTypes(untyped, rowType, pool_);
    }
};

} // namespace facebook::velox::exec::test
