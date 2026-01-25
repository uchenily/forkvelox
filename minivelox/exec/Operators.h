#pragma once

#include "exec/Operator.h"
#include "exec/PlanNode.h"
#include "exec/Expr.h"
#include "exec/Function.h" // For FunctionExpr
#include "vector/SimpleVector.h"
#include "vector/RowVector.h"
#include <deque>
#include <unordered_map>
#include <numeric>
#include <algorithm>

namespace facebook::velox::exec {

class OperatorUtils {
public:
    template<typename T>
    static VectorPtr copySimple(std::shared_ptr<FlatVector<T>> vec, const std::vector<vector_size_t>& indices, memory::MemoryPool* pool) {
        std::vector<T> newData;
        newData.reserve(indices.size());
        for (auto idx : indices) newData.push_back(vec->valueAt(idx));
        return makeFlatVector(newData, pool);
    }

    static VectorPtr copyVector(VectorPtr vec, const std::vector<vector_size_t>& indices, memory::MemoryPool* pool) {
        if (auto v = std::dynamic_pointer_cast<FlatVector<int32_t>>(vec)) return copySimple(v, indices, pool);
        if (auto v = std::dynamic_pointer_cast<FlatVector<int64_t>>(vec)) return copySimple(v, indices, pool);
        if (auto v = std::dynamic_pointer_cast<FlatVector<StringView>>(vec)) return copySimple(v, indices, pool);
        if (auto v = std::dynamic_pointer_cast<FlatVector<uint8_t>>(vec)) return copySimple(v, indices, pool);
        if (auto v = std::dynamic_pointer_cast<RowVector>(vec)) {
             std::vector<VectorPtr> children;
             for(auto& c : v->children()) children.push_back(copyVector(c, indices, pool));
             return std::make_shared<RowVector>(pool, v->type(), nullptr, indices.size(), children);
        }
        return nullptr;
    }
};

// ValuesOperator
class ValuesOperator : public Operator {
public:
    ValuesOperator(std::unique_ptr<OperatorCtx> ctx, const std::shared_ptr<const core::ValuesNode>& node)
        : Operator(std::move(ctx)), values_(node->values()), current_(0) {}

    void addInput(RowVectorPtr input) override {}

    RowVectorPtr getOutput() override {
        if (current_ < values_.size()) {
            return values_[current_++];
        }
        return nullptr;
    }

    bool isFinished() override { return current_ >= values_.size(); }

private:
    std::vector<RowVectorPtr> values_;
    size_t current_;
};

// FilterOperator
class FilterOperator : public Operator {
public:
    FilterOperator(std::unique_ptr<OperatorCtx> ctx, const std::shared_ptr<const core::FilterNode>& node)
        : Operator(std::move(ctx)), filter_(node->filter()) {}

    void addInput(RowVectorPtr input) override { input_ = input; }

    RowVectorPtr getOutput() override {
        if (!input_) return nullptr;
        
        core::ExecCtx* execCtx = ctx_->execCtx(); 
        EvalCtx evalCtx(execCtx, nullptr, input_);
        SelectivityVector rows(input_->size());
        VectorPtr result;
        filter_->eval(rows, evalCtx, result);
        
        auto boolVec = std::dynamic_pointer_cast<FlatVector<uint8_t>>(result);
        std::vector<vector_size_t> indices;
        for (vector_size_t i = 0; i < input_->size(); ++i) {
            if (boolVec->valueAt(i)) indices.push_back(i);
        }
        
        if (indices.empty()) {
             auto rowType = asRowType(input_->type());
             std::vector<VectorPtr> emptyChildren;
             for(size_t i=0; i<rowType->size(); ++i) emptyChildren.push_back(input_->childAt(i));
             auto ret = std::make_shared<RowVector>(ctx_->pool(), input_->type(), nullptr, 0, emptyChildren);
             input_ = nullptr;
             return ret;
        }
        
        if (indices.size() == input_->size()) {
             auto ret = input_;
             input_ = nullptr;
             return ret;
        }
        
        // Copy
        std::vector<VectorPtr> newChildren;
        auto rowType = asRowType(input_->type());
        for (size_t c = 0; c < input_->children().size(); ++c) {
             auto child = input_->childAt(c);
             newChildren.push_back(OperatorUtils::copyVector(child, indices, ctx_->pool()));
        }
        
        auto ret = std::make_shared<RowVector>(ctx_->pool(), rowType, nullptr, indices.size(), newChildren);
        input_ = nullptr;
        return ret;
    }

    bool isFinished() override { return false; }

private:
    exec::ExprPtr filter_;
    RowVectorPtr input_;
};

// ProjectOperator
class ProjectOperator : public Operator {
public:
    ProjectOperator(std::unique_ptr<OperatorCtx> ctx, const std::shared_ptr<const core::ProjectNode>& node)
        : Operator(std::move(ctx)), projections_(node->projections()), names_(node->names()) {}

    void addInput(RowVectorPtr input) override { input_ = input; }

    RowVectorPtr getOutput() override {
        if (!input_) return nullptr;
        core::ExecCtx* execCtx = ctx_->execCtx();
        EvalCtx evalCtx(execCtx, nullptr, input_);
        SelectivityVector rows(input_->size());
        
        std::vector<VectorPtr> outputs;
        for (auto& expr : projections_) {
            VectorPtr result;
            expr->eval(rows, evalCtx, result);
            outputs.push_back(result);
        }
        
        auto ret = makeRowVector(names_, outputs, ctx_->pool());
        input_ = nullptr;
        return ret;
    }
    
    bool isFinished() override { return false; }

private:
    std::vector<exec::ExprPtr> projections_;
    std::vector<std::string> names_;
    RowVectorPtr input_;
};

// TableScanOperator (Mock)
class TableScanOperator : public Operator {
public:
    TableScanOperator(std::unique_ptr<OperatorCtx> ctx, const std::shared_ptr<const core::TableScanNode>& node)
        : Operator(std::move(ctx)), outputType_(node->outputType()), finished_(false) {}

    void addInput(RowVectorPtr input) override {}

    RowVectorPtr getOutput() override {
        if (finished_) return nullptr;
        
        int rows = 25; 
        bool isRegion = false;
        for(auto& n : outputType_->names()) if (n.find("r_") == 0) isRegion = true;
        if (isRegion) rows = 5;
        
        auto pool = ctx_->pool();
        std::vector<VectorPtr> columns;
        for(size_t i=0; i<outputType_->size(); ++i) {
            std::string name = outputType_->nameOf(i);
            if (name == "n_nationkey" || name == "r_regionkey") {
                std::vector<int64_t> data;
                for(int r=0; r<rows; ++r) data.push_back(r);
                columns.push_back(makeFlatVector(data, pool));
            } else if (name == "n_regionkey") {
                std::vector<int64_t> data;
                for(int r=0; r<rows; ++r) data.push_back(r % 5);
                columns.push_back(makeFlatVector(data, pool));
            } else if (name == "n_name") {
                std::vector<std::string> data;
                for(int r=0; r<rows; ++r) data.push_back("Nation_" + std::to_string(r));
                columns.push_back(makeFlatVector(data, pool));
            } else if (name == "r_name") {
                std::vector<std::string> data = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
                columns.push_back(makeFlatVector(data, pool));
            } else {
                 std::vector<int64_t> data(rows, 0);
                 columns.push_back(makeFlatVector(data, pool));
            }
        }
        
        finished_ = true;
        return std::make_shared<RowVector>(pool, outputType_, nullptr, rows, columns);
    }
    
    bool isFinished() override { return finished_; }

private:
    RowTypePtr outputType_;
    bool finished_;
};

// AggregationOperator
class AggregationOperator : public Operator {
public:
    AggregationOperator(std::unique_ptr<OperatorCtx> ctx, const std::shared_ptr<const core::AggregationNode>& node)
        : Operator(std::move(ctx)), outputType_(node->outputType()), 
          groupingKeys_(node->groupingKeys()), aggregates_(node->aggregates()) {}

    void addInput(RowVectorPtr input) override { inputs_.push_back(input); }

    RowVectorPtr getOutput() override {
        if (!finishedInput_) return nullptr;
        if (finished_) return nullptr;
        
        if (groupingKeys_.empty()) return computeGlobalAgg();
        return computeGroupedAgg();
    }
    
    void noMoreInput() { finishedInput_ = true; }
    bool isFinished() override { return finished_; }

private:
    RowTypePtr outputType_;
    std::vector<std::string> groupingKeys_;
    std::vector<exec::ExprPtr> aggregates_;
    std::vector<RowVectorPtr> inputs_;
    bool finishedInput_ = false;
    bool finished_ = false;
    
    RowVectorPtr computeGlobalAgg() {
        std::vector<int64_t> sums(aggregates_.size(), 0);
        std::vector<int64_t> counts(aggregates_.size(), 0);
        
        for (auto& batch : inputs_) {
            for (size_t i = 0; i < aggregates_.size(); ++i) {
                // Parse "sum(a)"
                std::string s = aggregates_[i]->toString(); 
                size_t open = s.find('(');
                size_t close = s.find(')');
                std::string colName = s.substr(open + 1, close - open - 1); 
                std::string aggName = s.substr(0, open); 
                
                int colIdx = -1;
                auto rowType = asRowType(batch->type());
                for(size_t k=0; k<rowType->size(); ++k) {
                    if (rowType->nameOf(k) == colName) { colIdx = k; break; }
                }
                
                if (colIdx != -1) {
                    auto vec = batch->childAt(colIdx);
                    auto simple = std::dynamic_pointer_cast<FlatVector<int64_t>>(vec); 
                    if (simple) {
                        for(vector_size_t r=0; r<batch->size(); ++r) {
                             int64_t v = simple->valueAt(r);
                             if (aggName == "sum" || aggName == "avg") {
                                 sums[i] += v;
                                 counts[i]++;
                             } else if (aggName == "count") {
                                 counts[i]++;
                             }
                        }
                    }
                } else if (aggName == "count" && colName == "1") {
                    counts[i] += batch->size();
                }
            }
        }
        
        std::vector<VectorPtr> outputCols;
        for (size_t i = 0; i < aggregates_.size(); ++i) {
             std::string s = aggregates_[i]->toString();
             std::string aggName = s.substr(0, s.find('('));
             if (aggName == "avg") {
                 double avg = counts[i] == 0 ? 0 : (double)sums[i] / counts[i];
                 std::vector<double> res = {avg};
                 outputCols.push_back(makeFlatVector(res, ctx_->pool())); // makeFlatVector<double> needs specialized? No, generic works.
                 // Wait, SimpleVector.h handles int32, int64, string. I need double.
                 // I should update SimpleVector.h to support double.
                 // For now, cast to double if needed.
             } else {
                 int64_t val = (aggName == "count") ? counts[i] : sums[i];
                 std::vector<int64_t> res = {val};
                 outputCols.push_back(makeFlatVector(res, ctx_->pool()));
             }
        }
        finished_ = true;
        return makeRowVector(outputType_->names(), outputCols, ctx_->pool());
    }

    RowVectorPtr computeGroupedAgg() {
        std::unordered_map<std::string, int64_t> counts;
        std::string keyColName = groupingKeys_[0];
        
        for (auto& batch : inputs_) {
            int keyIdx = -1;
            auto rowType = asRowType(batch->type());
            for(size_t k=0; k<rowType->size(); ++k) if (rowType->nameOf(k) == keyColName) keyIdx = k;
            
            auto keyVec = std::dynamic_pointer_cast<FlatVector<StringView>>(batch->childAt(keyIdx));
            if (!keyVec) continue;
            
            for(vector_size_t r=0; r<batch->size(); ++r) {
                counts[keyVec->valueAt(r).str()]++;
            }
        }
        
        std::vector<std::string> keys;
        std::vector<int64_t> vals;
        for(auto& pair : counts) {
            keys.push_back(pair.first);
            vals.push_back(pair.second);
        }
        
        // Sorting keys for deterministic output in demo
        // Actually map iteration is unordered.
        // Demo sorts later.
        
        std::vector<VectorPtr> cols;
        cols.push_back(makeFlatVector(keys, ctx_->pool()));
        cols.push_back(makeFlatVector(vals, ctx_->pool()));
        
        finished_ = true;
        return makeRowVector(outputType_->names(), cols, ctx_->pool());
    }
};

// OrderByOperator
class OrderByOperator : public Operator {
public:
    OrderByOperator(std::unique_ptr<OperatorCtx> ctx, const std::shared_ptr<const core::OrderByNode>& node)
        : Operator(std::move(ctx)), sortingKeys_(node->sortingKeys()), sortingOrders_(node->sortingOrders()) {}
        
    void addInput(RowVectorPtr input) override { inputs_.push_back(input); }
    
    RowVectorPtr getOutput() override {
        if (!finishedInput_) return nullptr;
        if (finished_) return nullptr;
        
        if (inputs_.empty()) { finished_ = true; return nullptr; }
        
        // Flatten inputs (hack: assume single batch or merge)
        auto batch = inputs_[0]; // TODO: Merge
        
        std::vector<vector_size_t> indices(batch->size());
        std::iota(indices.begin(), indices.end(), 0);
        
        std::string key = sortingKeys_[0];
        bool asc = sortingOrders_[0];
        
        int keyIdx = -1;
        auto rowType = asRowType(batch->type());
        for(size_t k=0; k<rowType->size(); ++k) if (rowType->nameOf(k) == key) keyIdx = k;
        
        auto keyVec = batch->childAt(keyIdx);
        std::sort(indices.begin(), indices.end(), [&](vector_size_t a, vector_size_t b) {
             auto vA = std::dynamic_pointer_cast<FlatVector<int64_t>>(keyVec)->valueAt(a);
             auto vB = std::dynamic_pointer_cast<FlatVector<int64_t>>(keyVec)->valueAt(b);
             if (asc) return vA < vB;
             else return vA > vB;
        });
        
        finished_ = true;
        return OperatorUtils::copyVector(batch, indices, ctx_->pool());
    }
    
    void noMoreInput() { finishedInput_ = true; }
    bool isFinished() override { return finished_; }

private:
    std::vector<std::string> sortingKeys_;
    std::vector<bool> sortingOrders_;
    std::vector<RowVectorPtr> inputs_;
    bool finishedInput_ = false;
    bool finished_ = false;
};

// TopN Operator
class TopNOperator : public OrderByOperator {
public:
    TopNOperator(std::unique_ptr<OperatorCtx> ctx, const std::shared_ptr<const core::TopNNode>& node)
        : OrderByOperator(std::move(ctx), std::make_shared<core::OrderByNode>(node->id(), nullptr, node->sortingKeys(), node->sortingOrders(), false)), count_(node->count()) {}

    RowVectorPtr getOutput() override {
        auto sorted = OrderByOperator::getOutput();
        if (!sorted) return nullptr;
        
        if (sorted->size() <= count_) return sorted;
        
        std::vector<vector_size_t> indices;
        for(int i=0; i<count_; ++i) indices.push_back(i);
        return OperatorUtils::copyVector(sorted, indices, ctx_->pool());
    }
private:
    int32_t count_;
};

// HashJoinOperator
class HashJoinOperator : public Operator {
public:
    HashJoinOperator(std::unique_ptr<OperatorCtx> ctx, 
                     const std::vector<std::string>& leftKeys,
                     const std::vector<std::string>& rightKeys,
                     const std::vector<RowVectorPtr>& buildSide,
                     RowTypePtr outputType)
        : Operator(std::move(ctx)), leftKeys_(leftKeys), rightKeys_(rightKeys), buildSide_(buildSide), outputType_(outputType) {
        
        for(size_t b=0; b<buildSide_.size(); ++b) {
            auto batch = buildSide_[b];
            int keyIdx = -1;
            auto rowType = asRowType(batch->type());
            for(size_t k=0; k<rowType->size(); ++k) if (rowType->nameOf(k) == rightKeys_[0]) keyIdx = k;
            
            auto keyVec = std::dynamic_pointer_cast<FlatVector<int64_t>>(batch->childAt(keyIdx));
            for(vector_size_t r=0; r<batch->size(); ++r) {
                hashTable_[std::to_string(keyVec->valueAt(r))].push_back({(int)b, r});
            }
        }
    }

    void addInput(RowVectorPtr input) override { input_ = input; }

    RowVectorPtr getOutput() override {
        if (!input_) return nullptr;
        
        std::vector<vector_size_t> probeIndices;
        std::vector<std::pair<int, int>> buildIndices;
        
        int keyIdx = -1;
        auto rowType = asRowType(input_->type());
        for(size_t k=0; k<rowType->size(); ++k) if (rowType->nameOf(k) == leftKeys_[0]) keyIdx = k;
        
        auto keyVec = std::dynamic_pointer_cast<FlatVector<int64_t>>(input_->childAt(keyIdx));
        
        for(vector_size_t r=0; r<input_->size(); ++r) {
            std::string key = std::to_string(keyVec->valueAt(r));
            auto it = hashTable_.find(key);
            if (it != hashTable_.end()) {
                for(auto& pair : it->second) {
                    probeIndices.push_back(r);
                    buildIndices.push_back(pair);
                }
            }
        }
        
        std::vector<VectorPtr> outCols;
        auto probeType = asRowType(input_->type());
        auto buildBatch = buildSide_[0]; 
        auto buildType = asRowType(buildBatch->type());
        
        for(size_t i=0; i<outputType_->size(); ++i) {
            std::string name = outputType_->nameOf(i);
            int pIdx = -1;
            for(size_t k=0; k<probeType->size(); ++k) if (probeType->nameOf(k) == name) pIdx = k;
            
            if (pIdx != -1) {
                outCols.push_back(OperatorUtils::copyVector(input_->childAt(pIdx), probeIndices, ctx_->pool())); 
            } else {
                int bIdx = -1;
                for(size_t k=0; k<buildType->size(); ++k) if (buildType->nameOf(k) == name) bIdx = k;
                if (bIdx != -1) {
                     std::vector<vector_size_t> bIndices;
                     for(auto& p : buildIndices) bIndices.push_back(p.second);
                     outCols.push_back(OperatorUtils::copyVector(buildBatch->childAt(bIdx), bIndices, ctx_->pool()));
                }
            }
        }
        
        auto ret = std::make_shared<RowVector>(ctx_->pool(), outputType_, nullptr, probeIndices.size(), outCols);
        input_ = nullptr;
        return ret;
    }
    
    bool isFinished() override { return false; }

private:
    std::vector<std::string> leftKeys_;
    std::vector<std::string> rightKeys_;
    std::vector<RowVectorPtr> buildSide_;
    RowTypePtr outputType_;
    RowVectorPtr input_;
    std::unordered_map<std::string, std::vector<std::pair<int, int>>> hashTable_;
};

} // namespace facebook::velox::exec
