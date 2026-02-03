#pragma once
#include "velox/common/base/Exceptions.h"
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/RowVectorFile.h"
#include "velox/exec/LocalExchange.h"
#include "velox/expression/Expr.h"
#include "velox/functions/aggregates/AggregateFunction.h"
#include "velox/type/StringView.h"
#include "velox/type/Variant.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include <algorithm>
#include <cstring>
#include <functional>
#include <iostream>
#include <mutex>
#include <sstream>
#include <unordered_map>

namespace facebook::velox::exec {

class SourceState {
public:
  virtual ~SourceState() = default;
  virtual RowVectorPtr next() = 0;
};

class Operator {
public:
  Operator(core::PlanNodePtr planNode) : planNode_(planNode) {}
  virtual ~Operator() = default;

  virtual bool needsInput() const = 0;
  virtual void addInput(RowVectorPtr input) = 0;

  virtual void noMoreInput() { noMoreInput_ = true; }

  virtual RowVectorPtr getOutput() = 0;
  virtual bool isFinished() = 0;

  core::PlanNodePtr planNode() const { return planNode_; }

protected:
  core::PlanNodePtr planNode_;
  bool noMoreInput_ = false;
};

using OperatorSupplier = std::function<std::shared_ptr<Operator>(core::ExecCtx *)>;

class HashJoinBridge {
public:
  using BuildRow = std::pair<size_t, vector_size_t>;

  void addBuildBatch(const RowVectorPtr &batch) {
    if (!batch || batch->size() == 0) {
      return;
    }
    auto keyCol = std::dynamic_pointer_cast<SimpleVector<int64_t>>(batch->childAt(0));
    if (!keyCol) {
      VELOX_FAIL("HashJoin build side expects int64 key in column 0");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const size_t batchIndex = buildBatches_.size();
    buildBatches_.push_back(batch);
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      buildIndex_.emplace(keyCol->valueAt(row), BuildRow{batchIndex, row});
    }
  }

  void noMoreBuildInput() {
    std::lock_guard<std::mutex> lock(mutex_);
    buildFinished_ = true;
  }

  bool isBuildFinished() const { return buildFinished_; }

  const std::vector<RowVectorPtr> &buildBatches() const { return buildBatches_; }

  const std::unordered_multimap<int64_t, BuildRow> &buildIndex() const { return buildIndex_; }

private:
  std::vector<RowVectorPtr> buildBatches_;
  std::unordered_multimap<int64_t, BuildRow> buildIndex_;
  bool buildFinished_{false};
  mutable std::mutex mutex_;
};

class ValuesOperator : public Operator {
public:
  ValuesOperator(core::PlanNodePtr node, std::shared_ptr<SourceState> state = nullptr)
      : Operator(node), state_(std::move(state)) {
    if (!state_) {
      auto valuesNode = std::dynamic_pointer_cast<const core::ValuesNode>(node);
      values_ = valuesNode->values();
    }
  }
  void addInput(RowVectorPtr input) override {}
  void noMoreInput() override { noMoreInput_ = true; }
  RowVectorPtr getOutput() override {
    if (state_) {
      auto batch = state_->next();
      if (!batch) {
        finished_ = true;
      }
      return batch;
    }
    if (current_ < values_.size())
      return values_[current_++];
    return nullptr;
  }
  bool isFinished() override { return state_ ? finished_ : current_ >= values_.size(); }
  bool needsInput() const override { return false; }

private:
  std::vector<RowVectorPtr> values_;
  size_t current_ = 0;
  std::shared_ptr<SourceState> state_;
  bool finished_ = false;
};

class LocalExchangeSourceOperator : public Operator {
public:
  LocalExchangeSourceOperator(core::PlanNodePtr node, std::shared_ptr<LocalExchangeQueue> queue)
      : Operator(node), queue_(std::move(queue)) {}
  bool needsInput() const override { return false; }
  void addInput(RowVectorPtr input) override {}
  RowVectorPtr getOutput() override {
    if (finished_) {
      return nullptr;
    }
    RowVectorPtr batch;
    if (!queue_->dequeue(batch)) {
      finished_ = true;
      return nullptr;
    }
    return batch;
  }
  bool isFinished() override { return finished_; }

private:
  std::shared_ptr<LocalExchangeQueue> queue_;
  bool finished_ = false;
};

class LocalExchangeSinkOperator : public Operator {
public:
  LocalExchangeSinkOperator(core::PlanNodePtr node, std::shared_ptr<LocalExchangeQueue> queue)
      : Operator(node), queue_(std::move(queue)) {}
  bool needsInput() const override { return !noMoreInput_; }
  void addInput(RowVectorPtr input) override {
    if (input) {
      queue_->enqueue(input);
    }
  }
  void noMoreInput() override {
    noMoreInput_ = true;
    if (!finished_) {
      finished_ = true;
      queue_->producerFinished();
    }
  }
  RowVectorPtr getOutput() override { return nullptr; }
  bool isFinished() override { return finished_; }

private:
  std::shared_ptr<LocalExchangeQueue> queue_;
  bool finished_ = false;
};

class TableWriteOperator : public Operator {
public:
  TableWriteOperator(core::PlanNodePtr node) : Operator(node) {
    auto writeNode = std::dynamic_pointer_cast<const core::TableWriteNode>(node);
    path_ = writeNode->path();
  }
  bool needsInput() const override { return !noMoreInput_; }
  void addInput(RowVectorPtr input) override {
    if (!input || input->size() == 0) {
      return;
    }
    dwio::common::RowVectorFile::append(*input, path_, !wroteHeader_);
    wroteHeader_ = true;
  }
  void noMoreInput() override {
    noMoreInput_ = true;
    finished_ = true;
  }
  RowVectorPtr getOutput() override { return nullptr; }
  bool isFinished() override { return finished_; }

private:
  std::string path_;
  bool wroteHeader_ = false;
  bool finished_ = false;
};

class FileScanOperator : public Operator {
public:
  FileScanOperator(core::PlanNodePtr node, core::ExecCtx *ctx, std::shared_ptr<SourceState> state = nullptr)
      : Operator(node), ctx_(ctx), state_(std::move(state)) {
    auto scanNode = std::dynamic_pointer_cast<const core::FileScanNode>(node);
    path_ = scanNode->path();
    expectedType_ = scanNode->outputType();
  }
  bool needsInput() const override { return false; }
  void addInput(RowVectorPtr input) override {}
  RowVectorPtr getOutput() override {
    if (produced_) {
      return nullptr;
    }
    RowVectorPtr data;
    if (state_) {
      data = state_->next();
      if (!data) {
        finished_ = true;
      }
    } else {
      data = dwio::common::RowVectorFile::read(ctx_->pool(), path_);
    }
    if (!data) {
      produced_ = true;
      return nullptr;
    }
    if (expectedType_ && data && !expectedType_->equivalent(*data->type())) {
      VELOX_FAIL("File schema does not match expected output type");
    }
    if (!state_) {
      produced_ = true;
    }
    return data;
  }
  bool isFinished() override { return state_ ? finished_ : produced_; }

private:
  core::ExecCtx *ctx_;
  std::string path_;
  RowTypePtr expectedType_;
  bool produced_ = false;
  std::shared_ptr<SourceState> state_;
  bool finished_ = false;
};

class OrderByOperator : public Operator {
public:
  OrderByOperator(core::PlanNodePtr node) : Operator(node) {
    auto orderByNode = std::dynamic_pointer_cast<const core::OrderByNode>(node);
    isPartial_ = orderByNode->isPartial();
    for (const auto &key : orderByNode->keys()) {
      std::stringstream ss(key);
      std::string name, dir;
      ss >> name;
      if (ss >> dir)
        desc_.push_back(dir == "DESC");
      else
        desc_.push_back(false);
      columnNames_.push_back(name);
    }
  }
  bool needsInput() const override { return !noMoreInput_; }
  void addInput(RowVectorPtr input) override {
    if (input && input->size() > 0)
      batches_.push_back(input);
  }
  void noMoreInput() override {
    noMoreInput_ = true;
    finished_ = true;
  }
  RowVectorPtr getOutput() override {
    if (!finished_ || produced_)
      return nullptr;
    std::vector<std::pair<int, int>> rowIndices;
    size_t totalRows = 0;
    for (size_t i = 0; i < batches_.size(); ++i) {
      for (size_t j = 0; j < batches_[i]->size(); ++j)
        rowIndices.push_back({(int)i, (int)j});
      totalRows += batches_[i]->size();
    }
    if (totalRows == 0) {
      produced_ = true;
      return nullptr;
    }
    std::vector<int> colIndices;
    auto rowType = asRowType(batches_[0]->type());
    for (const auto &colName : columnNames_) {
      for (size_t i = 0; i < rowType->size(); ++i)
        if (rowType->nameOf(i) == colName) {
          colIndices.push_back(i);
          break;
        }
    }
    std::sort(rowIndices.begin(), rowIndices.end(), [&](const std::pair<int, int> &a, const std::pair<int, int> &b) {
      for (size_t k = 0; k < colIndices.size(); ++k) {
        int colIdx = colIndices[k];
        int cmp =
            batches_[a.first]->childAt(colIdx)->compare(batches_[b.first]->childAt(colIdx).get(), a.second, b.second);
        if (cmp != 0)
          return desc_[k] ? (cmp > 0) : (cmp < 0);
      }
      return false;
    });
    std::vector<VectorPtr> outCols;
    auto pool = batches_[0]->pool();
    for (size_t i = 0; i < rowType->size(); ++i) {
      auto type = rowType->childAt(i);
      VectorPtr col;
      if (type->kind() == TypeKind::BIGINT)
        col = std::make_shared<FlatVector<int64_t>>(pool, type, nullptr, totalRows,
                                                    AlignedBuffer::allocate(totalRows * sizeof(int64_t), pool));
      else
        col = std::make_shared<FlatVector<StringView>>(pool, type, nullptr, totalRows,
                                                       AlignedBuffer::allocate(totalRows * sizeof(StringView), pool));
      outCols.push_back(col);
    }
    auto result = std::make_shared<RowVector>(pool, rowType, nullptr, totalRows, outCols);
    for (size_t i = 0; i < totalRows; ++i)
      result->copy(batches_[rowIndices[i].first].get(), rowIndices[i].second, i);
    produced_ = true;
    return result;
  }
  bool isFinished() override { return produced_; }

private:
  std::vector<RowVectorPtr> batches_;
  std::vector<std::string> columnNames_;
  std::vector<bool> desc_;
  bool isPartial_{false};
  bool finished_ = false, produced_ = false;
};

class TopNOperator : public Operator {
public:
  TopNOperator(core::PlanNodePtr node) : Operator(node) {
    auto topNNode = std::dynamic_pointer_cast<const core::TopNNode>(node);
    limit_ = topNNode->count();
    for (const auto &key : topNNode->keys()) {
      std::stringstream ss(key);
      std::string name, dir;
      ss >> name;
      if (ss >> dir)
        desc_.push_back(dir == "DESC");
      else
        desc_.push_back(false);
      columnNames_.push_back(name);
    }
  }
  bool needsInput() const override { return !noMoreInput_; }
  void addInput(RowVectorPtr input) override {
    if (input && input->size() > 0)
      batches_.push_back(input);
  }
  void noMoreInput() override {
    noMoreInput_ = true;
    finished_ = true;
  }
  RowVectorPtr getOutput() override {
    if (!finished_ || produced_)
      return nullptr;
    std::vector<std::pair<int, int>> rowIndices;
    size_t totalRows = 0;
    for (size_t i = 0; i < batches_.size(); ++i) {
      for (size_t j = 0; j < batches_[i]->size(); ++j)
        rowIndices.push_back({(int)i, (int)j});
      totalRows += batches_[i]->size();
    }
    if (totalRows == 0) {
      produced_ = true;
      return nullptr;
    }
    std::vector<int> colIndices;
    auto rowType = asRowType(batches_[0]->type());
    for (const auto &colName : columnNames_) {
      for (size_t i = 0; i < rowType->size(); ++i)
        if (rowType->nameOf(i) == colName) {
          colIndices.push_back(i);
          break;
        }
    }
    std::sort(rowIndices.begin(), rowIndices.end(), [&](const std::pair<int, int> &a, const std::pair<int, int> &b) {
      for (size_t k = 0; k < colIndices.size(); ++k) {
        int colIdx = colIndices[k];
        int cmp =
            batches_[a.first]->childAt(colIdx)->compare(batches_[b.first]->childAt(colIdx).get(), a.second, b.second);
        if (cmp != 0)
          return desc_[k] ? (cmp > 0) : (cmp < 0);
      }
      return false;
    });
    if (limit_ >= 0 && totalRows > (size_t)limit_)
      totalRows = limit_;
    std::vector<VectorPtr> outCols;
    auto pool = batches_[0]->pool();
    for (size_t i = 0; i < rowType->size(); ++i) {
      auto type = rowType->childAt(i);
      VectorPtr col;
      if (type->kind() == TypeKind::BIGINT)
        col = std::make_shared<FlatVector<int64_t>>(pool, type, nullptr, totalRows,
                                                    AlignedBuffer::allocate(totalRows * sizeof(int64_t), pool));
      else
        col = std::make_shared<FlatVector<StringView>>(pool, type, nullptr, totalRows,
                                                       AlignedBuffer::allocate(totalRows * sizeof(StringView), pool));
      outCols.push_back(col);
    }
    auto result = std::make_shared<RowVector>(pool, rowType, nullptr, totalRows, outCols);
    for (size_t i = 0; i < totalRows; ++i)
      result->copy(batches_[rowIndices[i].first].get(), rowIndices[i].second, i);
    produced_ = true;
    return result;
  }
  bool isFinished() override { return produced_; }

private:
  std::vector<RowVectorPtr> batches_;
  std::vector<std::string> columnNames_;
  std::vector<bool> desc_;
  int limit_ = -1;
  bool finished_ = false, produced_ = false;
};

class FilterOperator : public Operator {
public:
  FilterOperator(core::PlanNodePtr node, core::ExecCtx *ctx) : Operator(node), ctx_(ctx) {
    auto filterNode = std::dynamic_pointer_cast<const core::FilterNode>(node);
    std::vector<core::TypedExprPtr> exprs = {filterNode->filter()};
    exprSet_ = std::make_unique<ExprSet>(exprs, ctx);
  }
  bool needsInput() const override { return !noMoreInput_; }
  void addInput(RowVectorPtr input) override {
    if (!input || input->size() == 0)
      return;
    EvalCtx evalCtx(ctx_, exprSet_.get(), input.get());
    SelectivityVector rows(input->size());
    std::vector<VectorPtr> results;
    exprSet_->eval(rows, evalCtx, results);
    auto filterVec = std::dynamic_pointer_cast<FlatVector<int32_t>>(results[0]);
    if (!filterVec)
      return;
    std::vector<int> selected;
    for (size_t i = 0; i < input->size(); ++i)
      if (filterVec->valueAt(i))
        selected.push_back(i);
    if (selected.empty())
      return;
    auto rowType = asRowType(input->type());
    std::vector<VectorPtr> children;
    auto pool = ctx_->pool();
    for (size_t col = 0; col < rowType->size(); ++col) {
      auto type = rowType->childAt(col);
      VectorPtr newCol;
      if (type->kind() == TypeKind::BIGINT)
        newCol = std::make_shared<FlatVector<int64_t>>(
            pool, type, nullptr, selected.size(), AlignedBuffer::allocate(selected.size() * sizeof(int64_t), pool));
      else if (type->kind() == TypeKind::VARCHAR)
        newCol = std::make_shared<FlatVector<StringView>>(
            pool, type, nullptr, selected.size(), AlignedBuffer::allocate(selected.size() * sizeof(StringView), pool));
      else
        newCol = std::make_shared<FlatVector<int32_t>>(
            pool, type, nullptr, selected.size(), AlignedBuffer::allocate(selected.size() * sizeof(int32_t), pool));
      for (size_t i = 0; i < selected.size(); ++i)
        newCol->copy(input->childAt(col).get(), selected[i], i);
      children.push_back(newCol);
    }
    input_ = std::make_shared<RowVector>(pool, rowType, nullptr, selected.size(), children);
  }
  void noMoreInput() override {
    noMoreInput_ = true;
    finished_ = true;
  }
  RowVectorPtr getOutput() override {
    auto result = input_;
    input_ = nullptr;
    return result;
  }
  bool isFinished() override { return finished_; }

private:
  RowVectorPtr input_;
  core::ExecCtx *ctx_;
  std::unique_ptr<ExprSet> exprSet_;
  bool finished_ = false;
};

class AggregationOperator : public Operator {
public:
  struct AggregateInfo {
    std::string func;
    std::string arg;
    std::string alias;
    std::shared_ptr<aggregate::AggregateFunction> function;
  };
  struct GroupKey {
    std::vector<Variant> values;
    bool operator==(const GroupKey &other) const {
      if (values.size() != other.values.size()) {
        return false;
      }
      for (size_t i = 0; i < values.size(); ++i) {
        if (!variantEquals(values[i], other.values[i])) {
          return false;
        }
      }
      return true;
    }
    static bool variantEquals(const Variant &a, const Variant &b) {
      if (a.kind() != b.kind())
        return false;
      if (a.isNull() || b.isNull())
        return a.isNull() && b.isNull();
      switch (a.kind()) {
      case TypeKind::BIGINT:
      case TypeKind::INTEGER:
        return a.value<int64_t>() == b.value<int64_t>();
      case TypeKind::VARCHAR:
        return a.value<std::string>() == b.value<std::string>();
      default:
        return false;
      }
    }
  };
  struct GroupKeyHash {
    size_t operator()(const GroupKey &key) const {
      size_t h = 0;
      for (const auto &v : key.values) {
        h ^= hashVariant(v) + 0x9e3779b9 + (h << 6) + (h >> 2);
      }
      return h;
    }
    static size_t hashVariant(const Variant &v) {
      size_t h = std::hash<int>()(static_cast<int>(v.kind()));
      if (v.isNull())
        return h;
      switch (v.kind()) {
      case TypeKind::BIGINT:
      case TypeKind::INTEGER:
        return h ^ std::hash<int64_t>()(v.value<int64_t>());
      case TypeKind::VARCHAR:
        return h ^ std::hash<std::string>()(v.value<std::string>());
      default:
        return h;
      }
    }
  };
  AggregationOperator(core::PlanNodePtr node, core::ExecCtx *ctx) : Operator(node), ctx_(ctx) {
    auto aggNode = std::dynamic_pointer_cast<const core::AggregationNode>(node);
    global_ = aggNode->groupingKeys().empty();
    groupingKeys_ = aggNode->groupingKeys();
    step_ = aggNode->step();
    for (const auto &agg : aggNode->aggregates()) {
      AggregateInfo info;
      size_t open = agg.find('('), close = agg.find(')'), asPos = agg.find(" AS ");
      if (open != std::string::npos && close != std::string::npos) {
        info.func = agg.substr(0, open);
        info.arg = agg.substr(open + 1, close - open - 1);
        if (asPos != std::string::npos)
          info.alias = agg.substr(asPos + 4);
        info.function = aggregate::getAggregateFunction(info.func);
        VELOX_CHECK(info.function, "Unknown aggregate function: {}", info.func);
        aggs_.push_back(info);
      }
    }
  }
  bool needsInput() const override { return !noMoreInput_; }
  void addInput(RowVectorPtr input) override {
    if (!input || input->size() == 0)
      return;
    hasInput_ = true;
    auto rowType = asRowType(input->type());
    if (!groupKeysInitialized_ && !groupingKeys_.empty()) {
      groupColIndices_.clear();
      groupingTypes_.clear();
      groupColIndices_.reserve(groupingKeys_.size());
      groupingTypes_.reserve(groupingKeys_.size());
      for (const auto &key : groupingKeys_) {
        bool found = false;
        for (int i = 0; i < rowType->size(); ++i) {
          if (rowType->nameOf(i) == key) {
            groupColIndices_.push_back(i);
            groupingTypes_.push_back(rowType->childAt(i));
            found = true;
            break;
          }
        }
        if (!found) {
          VELOX_FAIL("Grouping key '{}' not found in input row type.", key);
        }
      }
      groupKeysInitialized_ = true;
    }

    if (step_ == core::AggregationNode::Step::kFinal || step_ == core::AggregationNode::Step::kIntermediate) {
      std::vector<int> aggColIndices;
      std::vector<int> aggCountIndices;
      aggColIndices.reserve(aggs_.size());
      aggCountIndices.reserve(aggs_.size());
      for (const auto &info : aggs_) {
        int valueIdx = -1;
        int countIdx = -1;
        if (info.func == "avg") {
          std::string sumName = info.alias + "_sum";
          std::string countName = info.alias + "_count";
          for (int i = 0; i < rowType->size(); ++i) {
            if (rowType->nameOf(i) == sumName)
              valueIdx = i;
            if (rowType->nameOf(i) == countName)
              countIdx = i;
          }
        } else {
          for (int i = 0; i < rowType->size(); ++i) {
            if (rowType->nameOf(i) == info.alias) {
              valueIdx = i;
              break;
            }
          }
        }
        aggColIndices.push_back(valueIdx);
        aggCountIndices.push_back(countIdx);
      }

      for (vector_size_t i = 0; i < input->size(); ++i) {
        GroupKey key;
        if (!global_) {
          key.values.reserve(groupColIndices_.size());
          for (int idx : groupColIndices_) {
            key.values.push_back(readVariant(input->childAt(idx), i));
          }
        }
        auto [it, inserted] = groups_.try_emplace(key, GroupState{});
        if (inserted && !global_) {
          it->second.keyValues = key.values;
        }
        auto &group = it->second;
        if (group.aggResults.empty()) {
          group.aggResults.resize(aggs_.size());
        }
        for (size_t j = 0; j < aggs_.size(); ++j) {
          auto &info = aggs_[j];
          auto &res = group.aggResults[j];
          info.function->addIntermediate(input, i, aggColIndices[j], aggCountIndices[j], res);
        }
      }
      return;
    }

    if (!aggArgsInitialized_) {
      aggArgIndices_.clear();
      aggArgIndices_.reserve(aggs_.size());
      for (const auto &info : aggs_) {
        if (info.func == "count" && info.arg == "1") {
          aggArgIndices_.push_back(-1);
          continue;
        }
        bool found = false;
        for (int k = 0; k < rowType->size(); ++k) {
          if (rowType->nameOf(k) == info.arg) {
            aggArgIndices_.push_back(k);
            found = true;
            break;
          }
        }
        VELOX_CHECK(found, "Aggregation arg '{}' not found in input.", info.arg);
      }
      aggArgsInitialized_ = true;
    }

    for (vector_size_t i = 0; i < input->size(); ++i) {
      GroupKey key;
      if (!global_) {
        key.values.reserve(groupColIndices_.size());
        for (int idx : groupColIndices_) {
          key.values.push_back(readVariant(input->childAt(idx), i));
        }
      }
      auto [it, inserted] = groups_.try_emplace(key, GroupState{});
      if (inserted && !global_) {
        it->second.keyValues = key.values;
      }
      auto &group = it->second;
      if (group.aggResults.empty()) {
        group.aggResults.resize(aggs_.size());
      }
      for (size_t j = 0; j < aggs_.size(); ++j) {
        auto &info = aggs_[j];
        auto &res = group.aggResults[j];
        info.function->addRaw(input, i, aggArgIndices_[j], res);
      }
    }
  }
  void noMoreInput() override {
    noMoreInput_ = true;
    finished_ = true;
  }
  RowVectorPtr getOutput() override {
    if (!finished_ || produced_)
      return nullptr;
    if (!global_ && !hasInput_) {
      produced_ = true;
      return nullptr;
    }
    auto pool = ctx_->pool();
    std::vector<std::string> outNames = groupingKeys_;
    std::vector<TypePtr> outTypes;
    for (size_t k = 0; k < groupingKeys_.size(); ++k) {
      if (k < groupingTypes_.size())
        outTypes.push_back(groupingTypes_[k]);
      else
        outTypes.push_back(VARCHAR());
    }
    if (step_ == core::AggregationNode::Step::kPartial || step_ == core::AggregationNode::Step::kIntermediate) {
      for (const auto &info : aggs_) {
        if (info.function->usesSumAndCount()) {
          outNames.push_back(info.alias + "_sum");
          outTypes.push_back(BIGINT());
          outNames.push_back(info.alias + "_count");
          outTypes.push_back(BIGINT());
        } else {
          outNames.push_back(info.alias);
          outTypes.push_back(BIGINT());
        }
      }
    } else {
      for (const auto &info : aggs_) {
        outNames.push_back(info.alias);
        outTypes.push_back(BIGINT());
      }
    }
    size_t numGroups = groups_.size();
    if (global_ && numGroups == 0)
      numGroups = 1;
    std::vector<VectorPtr> outCols;
    for (size_t k = 0; k < groupingKeys_.size(); ++k) {
      auto typeKind = outTypes[k]->kind();
      if (typeKind == TypeKind::BIGINT) {
        auto col = std::make_shared<FlatVector<int64_t>>(pool, outTypes[k], nullptr, numGroups,
                                                         AlignedBuffer::allocate(numGroups * sizeof(int64_t), pool));
        int i = 0;
        for (auto &[key, group] : groups_) {
          col->mutableRawValues()[i++] = group.keyValues[k].value<int64_t>();
        }
        outCols.push_back(col);
      } else if (typeKind == TypeKind::INTEGER) {
        auto col = std::make_shared<FlatVector<int32_t>>(pool, outTypes[k], nullptr, numGroups,
                                                         AlignedBuffer::allocate(numGroups * sizeof(int32_t), pool));
        int i = 0;
        for (auto &[key, group] : groups_) {
          col->mutableRawValues()[i++] = static_cast<int32_t>(group.keyValues[k].value<int64_t>());
        }
        outCols.push_back(col);
      } else if (typeKind == TypeKind::VARCHAR) {
        auto values = AlignedBuffer::allocate(numGroups * sizeof(StringView), pool);
        auto col = std::make_shared<FlatVector<StringView>>(pool, outTypes[k], nullptr, numGroups, values);
        size_t totalLen = 0;
        for (auto &[key, group] : groups_) {
          totalLen += group.keyValues[k].value<std::string>().size();
        }
        auto dataBuffer = AlignedBuffer::allocate(totalLen, pool);
        char *bufPtr = dataBuffer->asMutable<char>();
        size_t offset = 0;
        int i = 0;
        for (auto &[key, group] : groups_) {
          const auto &str = group.keyValues[k].value<std::string>();
          std::memcpy(bufPtr + offset, str.data(), str.size());
          col->mutableRawValues()[i++] = StringView(bufPtr + offset, str.size());
          offset += str.size();
        }
        col->addStringBuffer(dataBuffer);
        outCols.push_back(col);
      } else {
        VELOX_FAIL("Unsupported grouping key type in AggregationOperator output.");
      }
    }
    for (size_t j = 0; j < aggs_.size(); ++j) {
      auto &info = aggs_[j];
      if (step_ == core::AggregationNode::Step::kPartial || step_ == core::AggregationNode::Step::kIntermediate) {
        if (info.function->usesSumAndCount()) {
          auto sumCol = std::make_shared<FlatVector<int64_t>>(
              pool, BIGINT(), nullptr, numGroups, AlignedBuffer::allocate(numGroups * sizeof(int64_t), pool));
          auto countCol = std::make_shared<FlatVector<int64_t>>(
              pool, BIGINT(), nullptr, numGroups, AlignedBuffer::allocate(numGroups * sizeof(int64_t), pool));
          int i = 0;
          if (groups_.empty() && global_) {
            sumCol->mutableRawValues()[0] = 0;
            countCol->mutableRawValues()[0] = 0;
          } else {
            for (auto &[key, group] : groups_) {
              auto &res = group.aggResults[j];
              sumCol->mutableRawValues()[i] = res.sum;
              countCol->mutableRawValues()[i] = res.count;
              ++i;
            }
          }
          outCols.push_back(sumCol);
          outCols.push_back(countCol);
        } else {
          auto col = std::make_shared<FlatVector<int64_t>>(pool, BIGINT(), nullptr, numGroups,
                                                           AlignedBuffer::allocate(numGroups * sizeof(int64_t), pool));
          int i = 0;
          if (groups_.empty() && global_)
            col->mutableRawValues()[0] = 0;
          else {
            for (auto &[key, group] : groups_) {
              auto &res = group.aggResults[j];
              if (info.func == "sum")
                col->mutableRawValues()[i++] = res.sum;
              else
                col->mutableRawValues()[i++] = res.count;
            }
          }
          outCols.push_back(col);
        }
      } else {
        auto col = std::make_shared<FlatVector<int64_t>>(pool, BIGINT(), nullptr, numGroups,
                                                         AlignedBuffer::allocate(numGroups * sizeof(int64_t), pool));
        int i = 0;
        if (groups_.empty() && global_)
          col->mutableRawValues()[0] = 0;
        else {
          for (auto &[key, group] : groups_) {
            auto &res = group.aggResults[j];
            col->mutableRawValues()[i++] = info.function->finalize(res);
          }
        }
        outCols.push_back(col);
      }
    }
    auto rowType = ROW(outNames, outTypes);
    produced_ = true;
    return std::make_shared<RowVector>(pool, rowType, nullptr, numGroups, outCols);
  }
  bool isFinished() override { return produced_; }

private:
  using AggRes = aggregate::AggregateAccumulator;
  struct GroupState {
    std::vector<AggRes> aggResults;
    std::vector<Variant> keyValues;
  };
  static Variant readVariant(const VectorPtr &vec, vector_size_t row) {
    if (vec->isNullAt(row)) {
      return Variant(vec->type()->kind());
    }
    switch (vec->type()->kind()) {
    case TypeKind::BIGINT: {
      auto simple = std::dynamic_pointer_cast<SimpleVector<int64_t>>(vec);
      if (!simple)
        VELOX_FAIL("Expected BIGINT vector for grouping key.");
      return Variant(simple->valueAt(row));
    }
    case TypeKind::INTEGER: {
      auto simple = std::dynamic_pointer_cast<SimpleVector<int32_t>>(vec);
      if (!simple)
        VELOX_FAIL("Expected INTEGER vector for grouping key.");
      return Variant(simple->valueAt(row));
    }
    case TypeKind::VARCHAR: {
      auto simple = std::dynamic_pointer_cast<SimpleVector<StringView>>(vec);
      if (!simple)
        VELOX_FAIL("Expected VARCHAR vector for grouping key.");
      auto sv = simple->valueAt(row);
      return Variant(std::string(sv.data(), sv.size()));
    }
    default:
      VELOX_FAIL("Unsupported grouping key type in AggregationOperator.");
    }
  }
  std::vector<AggregateInfo> aggs_;
  std::vector<std::string> groupingKeys_;
  std::vector<int> groupColIndices_;
  std::vector<TypePtr> groupingTypes_;
  std::unordered_map<GroupKey, GroupState, GroupKeyHash> groups_;
  core::ExecCtx *ctx_;
  bool global_ = true, hasInput_ = false, finished_ = false, produced_ = false;
  bool groupKeysInitialized_ = false;
  std::vector<int> aggArgIndices_;
  bool aggArgsInitialized_ = false;
  core::AggregationNode::Step step_{core::AggregationNode::Step::kSingle};
};

class PassThroughOperator : public Operator {
public:
  PassThroughOperator(core::PlanNodePtr node) : Operator(node) {}
  bool needsInput() const override { return !noMoreInput_; }
  void addInput(RowVectorPtr input) override { input_ = input; }
  void noMoreInput() override {
    noMoreInput_ = true;
    finished_ = true;
  }
  RowVectorPtr getOutput() override {
    auto res = input_;
    input_ = nullptr;
    return res;
  }
  bool isFinished() override { return finished_; }

private:
  RowVectorPtr input_;
  bool finished_ = false;
};

class HashBuildOperator : public Operator {
public:
  HashBuildOperator(core::PlanNodePtr node, std::shared_ptr<HashJoinBridge> bridge)
      : Operator(node), bridge_(std::move(bridge)) {}
  bool needsInput() const override { return !noMoreInput_; }
  void addInput(RowVectorPtr input) override {
    if (bridge_) {
      bridge_->addBuildBatch(input);
    }
  }
  void noMoreInput() override {
    noMoreInput_ = true;
    if (bridge_) {
      bridge_->noMoreBuildInput();
    }
    finished_ = true;
  }
  RowVectorPtr getOutput() override { return nullptr; }
  bool isFinished() override { return finished_; }

private:
  std::shared_ptr<HashJoinBridge> bridge_;
  bool finished_ = false;
};

class HashProbeOperator : public Operator {
public:
  HashProbeOperator(core::PlanNodePtr node, core::ExecCtx *ctx, std::shared_ptr<HashJoinBridge> bridge)
      : Operator(node), ctx_(ctx), bridge_(std::move(bridge)) {}
  bool needsInput() const override { return !noMoreInput_; }
  void addInput(RowVectorPtr input) override {
    if (!input || input->size() == 0 || !bridge_) {
      return;
    }
    pendingOutput_ = buildOutput(input);
  }
  void noMoreInput() override {
    noMoreInput_ = true;
    if (!pendingOutput_) {
      finished_ = true;
    }
  }
  RowVectorPtr getOutput() override {
    auto output = pendingOutput_;
    pendingOutput_ = nullptr;
    if (noMoreInput_ && !output) {
      finished_ = true;
    }
    return output;
  }
  bool isFinished() override { return finished_; }

private:
  struct MatchRow {
    vector_size_t probeRow;
    HashJoinBridge::BuildRow buildRow;
  };

  RowVectorPtr buildOutput(const RowVectorPtr &probeBatch) {
    auto probeKeyCol = std::dynamic_pointer_cast<SimpleVector<int64_t>>(probeBatch->childAt(0));
    if (!probeKeyCol) {
      VELOX_FAIL("HashJoin probe side expects int64 key in column 0");
    }
    std::vector<MatchRow> matches;
    const auto &index = bridge_->buildIndex();
    for (vector_size_t row = 0; row < probeBatch->size(); ++row) {
      auto range = index.equal_range(probeKeyCol->valueAt(row));
      for (auto it = range.first; it != range.second; ++it) {
        matches.push_back(MatchRow{row, it->second});
      }
    }
    if (matches.empty()) {
      return nullptr;
    }
    auto pool = ctx_->pool();
    auto keyCol = std::make_shared<FlatVector<int64_t>>(
        pool, BIGINT(), nullptr, matches.size(), AlignedBuffer::allocate(matches.size() * sizeof(int64_t), pool));
    auto outCol = std::make_shared<FlatVector<StringView>>(
        pool, VARCHAR(), nullptr, matches.size(), AlignedBuffer::allocate(matches.size() * sizeof(StringView), pool));
    const auto &buildBatches = bridge_->buildBatches();
    for (size_t i = 0; i < matches.size(); ++i) {
      const auto &match = matches[i];
      keyCol->mutableRawValues()[i] = probeKeyCol->valueAt(match.probeRow);
      auto buildBatch = buildBatches[match.buildRow.first];
      auto nameCol = std::dynamic_pointer_cast<SimpleVector<StringView>>(buildBatch->childAt(1));
      outCol->copy(nameCol.get(), match.buildRow.second, i);
    }
    return std::make_shared<RowVector>(pool, ROW({"join_key", "r_name"}, {BIGINT(), VARCHAR()}), nullptr,
                                       matches.size(), std::vector<VectorPtr>{keyCol, outCol});
  }

  RowVectorPtr pendingOutput_;
  core::ExecCtx *ctx_;
  std::shared_ptr<HashJoinBridge> bridge_;
  bool finished_ = false;
};

} // namespace facebook::velox::exec
