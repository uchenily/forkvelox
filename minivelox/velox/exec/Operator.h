#pragma once
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/type/StringView.h"
#include "velox/core/PlanNode.h"
#include "velox/expression/Expr.h"
#include <iostream>
#include <algorithm>
#include <sstream>

namespace facebook::velox::exec {

class Operator {
public:
    Operator(core::PlanNodePtr planNode) : planNode_(planNode) {}
    virtual ~Operator() = default;
    
    virtual void addInput(RowVectorPtr input) = 0;
    virtual RowVectorPtr getOutput() = 0;
    virtual bool isFinished() = 0;
    virtual bool needsInput() const { return true; }
    
    core::PlanNodePtr planNode() const { return planNode_; }

protected:
    core::PlanNodePtr planNode_;
};

class ValuesOperator : public Operator {
public:
    ValuesOperator(core::PlanNodePtr node) : Operator(node) {
        auto valuesNode = std::dynamic_pointer_cast<const core::ValuesNode>(node);
        values_ = valuesNode->values();
    }
    
    void addInput(RowVectorPtr input) override {
        // Source operator, no input
    }
    
    RowVectorPtr getOutput() override {
        if (current_ < values_.size()) {
            return values_[current_++];
        }
        return nullptr;
    }
    
    bool isFinished() override { return current_ >= values_.size(); }
    bool needsInput() const override { return false; }
    
private:
    std::vector<RowVectorPtr> values_;
    size_t current_ = 0;
};

class OrderByOperator : public Operator {
public:
    OrderByOperator(core::PlanNodePtr node) : Operator(node) {
        auto orderByNode = std::dynamic_pointer_cast<const core::OrderByNode>(node);
        // Parse keys: "a DESC" -> "a", true
        for(const auto& key : orderByNode->keys()) {
            std::stringstream ss(key);
            std::string name;
            std::string dir;
            ss >> name;
            if (ss >> dir) {
                desc_.push_back(dir == "DESC");
            } else {
                desc_.push_back(false);
            }
            columnNames_.push_back(name);
        }
    }
    
    void addInput(RowVectorPtr input) override {
        if (input && input->size() > 0) {
            batches_.push_back(input);
        }
    }
    
    RowVectorPtr getOutput() override {
        if (!finished_ || produced_) return nullptr;
        
        // 1. Collect all rows
        std::vector<std::pair<int, int>> rowIndices;
        size_t totalRows = 0;
        for(size_t i=0; i<batches_.size(); ++i) {
            size_t sz = batches_[i]->size();
            for(size_t j=0; j<sz; ++j) {
                rowIndices.push_back({(int)i, (int)j});
            }
            totalRows += sz;
        }
        
        if (totalRows == 0) {
            produced_ = true;
            return nullptr;
        }
        
        // 2. Identify column indices for sorting
        std::vector<int> colIndices;
        if (!batches_.empty()) {
            auto rowType = std::dynamic_pointer_cast<const RowType>(batches_[0]->type());
            auto& names = rowType->names();
            for(const auto& colName : columnNames_) {
                for(size_t i=0; i<names.size(); ++i) {
                    if (names[i] == colName) {
                        colIndices.push_back(i);
                        break;
                    }
                }
            }
        }
        
        // 3. Sort
        std::sort(rowIndices.begin(), rowIndices.end(), [&](const std::pair<int, int>& a, const std::pair<int, int>& b) {
            for(size_t k=0; k<colIndices.size(); ++k) {
                int colIdx = colIndices[k];
                auto& batchA = batches_[a.first];
                auto& batchB = batches_[b.first];
                auto& vecA = batchA->children()[colIdx];
                auto& vecB = batchB->children()[colIdx];
                
                int cmp = vecA->compare(vecB.get(), a.second, b.second);
                if (cmp != 0) {
                    return desc_[k] ? (cmp > 0) : (cmp < 0);
                }
            }
            return false;
        });
        
        // 4. Construct output
        // Allocate new columns
        std::vector<VectorPtr> outputCols;
        auto rowType = std::dynamic_pointer_cast<const RowType>(batches_[0]->type());
        
        for(size_t i=0; i<rowType->size(); ++i) {
            // Allocate FlatVector for each column
            auto type = rowType->childAt(i);
            // Assuming FlatVector for demo types (int64, stringview)
            // We need a generic createVector(type, size) helper, or manual switch
            VectorPtr col;
            auto pool = batches_[0]->pool(); // Borrow pool from first batch
            
            if (type->kind() == TypeKind::BIGINT) {
                col = std::make_shared<FlatVector<int64_t>>(pool, type, nullptr, totalRows, 
                        AlignedBuffer::allocate(totalRows * sizeof(int64_t), pool));
            } else if (type->kind() == TypeKind::VARCHAR) {
                col = std::make_shared<FlatVector<StringView>>(pool, type, nullptr, totalRows, 
                        AlignedBuffer::allocate(totalRows * sizeof(StringView), pool));
            } else {
                VELOX_FAIL("Unsupported sort output type");
            }
            outputCols.push_back(col);
        }
        
        auto output = std::make_shared<RowVector>(batches_[0]->pool(), rowType, nullptr, totalRows, outputCols);
        
        // Copy data
        for(size_t i=0; i<totalRows; ++i) {
            auto& idx = rowIndices[i];
            output->copy(batches_[idx.first].get(), idx.second, i);
        }
        
        produced_ = true;
        return output;
    }
    
    bool isFinished() override { return produced_; }
    void noMoreInput() { finished_ = true; }
    
private:
    std::vector<RowVectorPtr> batches_;
    std::vector<std::string> columnNames_;
    std::vector<bool> desc_;
    bool finished_ = false;
    bool produced_ = false;
};

class TopNOperator : public Operator {
public:
    TopNOperator(core::PlanNodePtr node) : Operator(node) {
        auto topNNode = std::dynamic_pointer_cast<const core::TopNNode>(node);
        limit_ = topNNode->count();
        for(const auto& key : topNNode->keys()) {
            std::stringstream ss(key);
            std::string name;
            std::string dir;
            ss >> name;
            if (ss >> dir) {
                desc_.push_back(dir == "DESC");
            } else {
                desc_.push_back(false);
            }
            columnNames_.push_back(name);
        }
    }
    
    void addInput(RowVectorPtr input) override {
        if (input && input->size() > 0) batches_.push_back(input);
    }
    
    RowVectorPtr getOutput() override {
        if (!finished_ || produced_) return nullptr;
        
        // 1. Collect all rows
        std::vector<std::pair<int, int>> rowIndices;
        size_t totalRows = 0;
        for(size_t i=0; i<batches_.size(); ++i) {
            size_t sz = batches_[i]->size();
            for(size_t j=0; j<sz; ++j) {
                rowIndices.push_back({(int)i, (int)j});
            }
            totalRows += sz;
        }
        
        if (totalRows == 0) {
            produced_ = true;
            return nullptr;
        }
        
        // 2. Identify column indices for sorting
        std::vector<int> colIndices;
        if (!batches_.empty()) {
            auto rowType = std::dynamic_pointer_cast<const RowType>(batches_[0]->type());
            auto& names = rowType->names();
            for(const auto& colName : columnNames_) {
                for(size_t i=0; i<names.size(); ++i) {
                    if (names[i] == colName) {
                        colIndices.push_back(i);
                        break;
                    }
                }
            }
        }
        
        // 3. Sort
        std::sort(rowIndices.begin(), rowIndices.end(), [&](const std::pair<int, int>& a, const std::pair<int, int>& b) {
            for(size_t k=0; k<colIndices.size(); ++k) {
                int colIdx = colIndices[k];
                auto& batchA = batches_[a.first];
                auto& batchB = batches_[b.first];
                auto& vecA = batchA->children()[colIdx];
                auto& vecB = batchB->children()[colIdx];
                
                int cmp = vecA->compare(vecB.get(), a.second, b.second);
                if (cmp != 0) {
                    return desc_[k] ? (cmp > 0) : (cmp < 0);
                }
            }
            return false;
        });
        
        // 4. Truncate for TopN
        if (limit_ >= 0 && totalRows > (size_t)limit_) {
            totalRows = limit_;
        }
        
        // 5. Construct output
        std::vector<VectorPtr> outputCols;
        auto rowType = std::dynamic_pointer_cast<const RowType>(batches_[0]->type());
        
        for(size_t i=0; i<rowType->size(); ++i) {
            auto type = rowType->childAt(i);
            VectorPtr col;
            auto pool = batches_[0]->pool();
            
            if (type->kind() == TypeKind::BIGINT) {
                col = std::make_shared<FlatVector<int64_t>>(pool, type, nullptr, totalRows, 
                        AlignedBuffer::allocate(totalRows * sizeof(int64_t), pool));
            } else if (type->kind() == TypeKind::VARCHAR) {
                col = std::make_shared<FlatVector<StringView>>(pool, type, nullptr, totalRows, 
                        AlignedBuffer::allocate(totalRows * sizeof(StringView), pool));
            } else {
                VELOX_FAIL("Unsupported sort output type");
            }
            outputCols.push_back(col);
        }
        
        auto output = std::make_shared<RowVector>(batches_[0]->pool(), rowType, nullptr, totalRows, outputCols);
        
        for(size_t i=0; i<totalRows; ++i) {
            auto& idx = rowIndices[i];
            output->copy(batches_[idx.first].get(), idx.second, i);
        }
        
        produced_ = true;
        return output;
    }
    
    bool isFinished() override { return produced_; }
    void noMoreInput() { finished_ = true; }

private:
    std::vector<RowVectorPtr> batches_;
    std::vector<std::string> columnNames_;
    std::vector<bool> desc_;
    int limit_ = -1;
    bool finished_ = false;
    bool produced_ = false;
};

class FilterOperator : public Operator {
public:
    FilterOperator(core::PlanNodePtr node, core::ExecCtx* ctx) : Operator(node), ctx_(ctx) {
        auto filterNode = std::dynamic_pointer_cast<const core::FilterNode>(node);
        std::vector<core::TypedExprPtr> exprs = {filterNode->filter()};
        exprSet_ = std::make_unique<ExprSet>(exprs, ctx);
    }
    
    void addInput(RowVectorPtr input) override {
        input_ = input;
        if (input_) {
            EvalCtx evalCtx(ctx_, exprSet_.get(), input_.get());
            SelectivityVector rows(input_->size());
            std::vector<VectorPtr> results;
            exprSet_->eval(rows, evalCtx, results);
            // Stub filter logic: We should filter based on results[0] (boolean)
            // For now, pass through as stub or assume true
        }
    }
    
    RowVectorPtr getOutput() override {
        auto result = input_;
        input_ = nullptr;
        return result; 
    }
    
    bool isFinished() override { return finished_; }
    void noMoreInput() { finished_ = true; }
    
private:
    RowVectorPtr input_;
    core::ExecCtx* ctx_;
    std::unique_ptr<ExprSet> exprSet_;
    bool finished_ = false;
};

class PassThroughOperator : public Operator {
public:
    PassThroughOperator(core::PlanNodePtr node) : Operator(node) {}
    
    void addInput(RowVectorPtr input) override {
        input_ = input;
    }
    
    RowVectorPtr getOutput() override {
        auto res = input_;
        input_ = nullptr;
        return res;
    }
    
    bool isFinished() override { return finished_; }
    void noMoreInput() { finished_ = true; }
    
private:
    RowVectorPtr input_;
    bool finished_ = false;
};

}
