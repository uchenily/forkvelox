#pragma once
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/core/ExecCtx.h"
#include "velox/exec/Split.h"
#include "velox/tpch/gen/TpchGen.h"
#include <unordered_map>

namespace facebook::velox::exec::test {

class AssertQueryBuilder {
public:
    AssertQueryBuilder(core::PlanNodePtr planNode) : planNode_(planNode) {}
    
    std::shared_ptr<RowVector> copyResults(memory::MemoryPool* pool) {
        auto queryCtx = core::QueryCtx::create();
        core::ExecCtx execCtx(pool, queryCtx.get());
        std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>> bridges;
        buildAllJoins(planNode_, &execCtx, bridges);

        std::vector<std::shared_ptr<Operator>> ops;
        buildPipeline(planNode_, ops, &execCtx, bridges);
        std::reverse(ops.begin(), ops.end());

        ::facebook::velox::exec::Driver driver(ops);
        auto batches = driver.run();
        
        if (batches.empty()) {
             return nullptr; 
        }
        
        return std::dynamic_pointer_cast<RowVector>(batches[0]);
    }
    
    AssertQueryBuilder& split(const core::PlanNodeId& id, exec::Split split) {
        return *this;
    }
    
    AssertQueryBuilder& split(exec::Split split) {
        return *this;
    }

private:
    RowVectorPtr createTpchNation(memory::MemoryPool* pool) {
        std::vector<int64_t> keys = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24};
        std::vector<std::string> names = {"ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"};
        std::vector<int64_t> regionKeys = {0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 4, 0, 1, 2, 3, 4, 2, 3, 3, 1};
        
        auto keyVec = makeFlatVector<int64_t>(pool, keys);
        auto nameVec = makeFlatVectorString(pool, names);
        auto rKeyVec = makeFlatVector<int64_t>(pool, regionKeys);
        
        return std::make_shared<RowVector>(pool, ROW({"n_nationkey", "n_name", "n_regionkey"}, {BIGINT(), VARCHAR(), BIGINT()}), nullptr, 25, std::vector<VectorPtr>{keyVec, nameVec, rKeyVec});
    }

    RowVectorPtr createTpchRegion(memory::MemoryPool* pool) {
        std::vector<int64_t> keys = {0, 1, 2, 3, 4};
        std::vector<std::string> names = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
        
        auto keyVec = makeFlatVector<int64_t>(pool, keys);
        auto nameVec = makeFlatVectorString(pool, names);
        
        return std::make_shared<RowVector>(pool, ROW({"r_regionkey", "r_name"}, {BIGINT(), VARCHAR()}), nullptr, 5, std::vector<VectorPtr>{keyVec, nameVec});
    }

    template<typename T>
    std::shared_ptr<FlatVector<T>> makeFlatVector(memory::MemoryPool* pool, const std::vector<T>& data) {
        auto buf = AlignedBuffer::allocate(data.size() * sizeof(T), pool);
        std::memcpy(buf->as_mutable_uint8_t(), data.data(), data.size() * sizeof(T));
        return std::make_shared<FlatVector<T>>(pool, (sizeof(T)==8 ? BIGINT() : INTEGER()), nullptr, data.size(), buf);
    }

    std::shared_ptr<FlatVector<StringView>> makeFlatVectorString(memory::MemoryPool* pool, const std::vector<std::string>& data) {
        size_t size = data.size();
        auto values = AlignedBuffer::allocate(size * sizeof(StringView), pool);
        auto* rawValues = values->asMutable<StringView>();
        size_t totalLen = 0;
        for(const auto& s : data) totalLen += s.size();
        auto dataBuffer = AlignedBuffer::allocate(totalLen, pool);
        char* bufPtr = dataBuffer->asMutable<char>();
        size_t offset = 0;
        for(size_t i=0; i<size; ++i) {
            std::memcpy(bufPtr + offset, data[i].data(), data[i].size());
            rawValues[i] = StringView(bufPtr + offset, data[i].size());
            offset += data[i].size();
        }
        auto vec = std::make_shared<FlatVector<StringView>>(pool, VARCHAR(), nullptr, size, values);
        vec->addStringBuffer(dataBuffer);
        return vec;
    }

    void buildPipeline(
        core::PlanNodePtr node,
        std::vector<std::shared_ptr<Operator>>& ops,
        core::ExecCtx* ctx,
        const std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>>& bridges) {
        if (auto values = std::dynamic_pointer_cast<const core::ValuesNode>(node)) {
            ops.push_back(std::make_shared<ValuesOperator>(node));
        } else if (std::dynamic_pointer_cast<const core::FileScanNode>(node)) {
            ops.push_back(std::make_shared<FileScanOperator>(node, ctx));
        } else if (std::dynamic_pointer_cast<const core::TableWriteNode>(node)) {
            ops.push_back(std::make_shared<TableWriteOperator>(node));
        } else if (auto filter = std::dynamic_pointer_cast<const core::FilterNode>(node)) {
            ops.push_back(std::make_shared<FilterOperator>(node, ctx));
        } else if (auto agg = std::dynamic_pointer_cast<const core::AggregationNode>(node)) {
            ops.push_back(std::make_shared<AggregationOperator>(node, ctx));
        } else if (auto orderBy = std::dynamic_pointer_cast<const core::OrderByNode>(node)) {
            ops.push_back(std::make_shared<OrderByOperator>(node));
        } else if (auto topN = std::dynamic_pointer_cast<const core::TopNNode>(node)) {
            ops.push_back(std::make_shared<TopNOperator>(node));
        } else if (auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
            auto it = bridges.find(join->id());
            if (it == bridges.end()) {
                VELOX_FAIL("Missing HashJoin bridge for plan node");
            }
            ops.push_back(std::make_shared<HashProbeOperator>(node, ctx, it->second));
        } else if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(node)) {
            RowVectorPtr data;
            if (scan->table() == tpch::Table::TBL_NATION) data = createTpchNation(ctx->pool());
            else data = createTpchRegion(ctx->pool());
            
            std::vector<VectorPtr> children;
            std::vector<std::string> names;
            std::vector<TypePtr> types;
            auto fullRowType = asRowType(data->type());
            for (const auto& col : scan->columns()) {
                for (int i = 0; i < fullRowType->size(); ++i) {
                    if (fullRowType->nameOf(i) == col) {
                        children.push_back(data->childAt(i));
                        names.push_back(col);
                        types.push_back(fullRowType->childAt(i));
                        break;
                    }
                }
            }
            auto projectedData = std::make_shared<RowVector>(ctx->pool(), ROW(names, types), nullptr, data->size(), children);
            ops.push_back(std::make_shared<ValuesOperator>(
                std::make_shared<core::ValuesNode>(scan->id(), std::vector<RowVectorPtr>{projectedData}))); 
        } else {
             ops.push_back(std::make_shared<PassThroughOperator>(node));
        }
        
        auto sources = node->sources();
        if (!sources.empty()) {
            buildPipeline(sources[0], ops, ctx, bridges);
        }
    }

    void buildAllJoins(
        const core::PlanNodePtr& node,
        core::ExecCtx* ctx,
        std::unordered_map<core::PlanNodeId, std::shared_ptr<HashJoinBridge>>& bridges) {
        if (!node) {
            return;
        }
        for (const auto& source : node->sources()) {
            buildAllJoins(source, ctx, bridges);
        }
        auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(node);
        if (!join) {
            return;
        }
        if (bridges.find(join->id()) != bridges.end()) {
            return;
        }
        auto bridge = std::make_shared<HashJoinBridge>();
        bridges.emplace(join->id(), bridge);
        std::vector<std::shared_ptr<Operator>> buildOps;
        buildPipeline(join->sources()[1], buildOps, ctx, bridges);
        std::reverse(buildOps.begin(), buildOps.end());
        buildOps.push_back(std::make_shared<HashBuildOperator>(node, bridge));
        ::facebook::velox::exec::Driver buildDriver(buildOps);
        buildDriver.run();
    }

    core::PlanNodePtr planNode_;
};

}
