#pragma once
#include "velox/core/ITypedExpr.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/ComplexVector.h"
#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace facebook::velox::core {

using PlanNodeId = std::string;

class PlanNode {
public:
  virtual ~PlanNode() = default;
  virtual const PlanNodeId &id() const = 0;
  virtual std::string toString() const = 0;
  std::string toString(bool detailed, bool recursive) const {
    std::stringstream stream;
    toStringInternal(stream, detailed, recursive, 0);
    return stream.str();
  }
  virtual std::vector<std::shared_ptr<const PlanNode>> sources() const = 0;

protected:
  virtual void addDetails(std::stringstream & /*stream*/) const {}

private:
  void toStringInternal(std::stringstream &stream, bool detailed, bool recursive, size_t indentationSize) const {
    stream << std::string(indentationSize, ' ') << "-> " << this->toString();
    if (detailed) {
      stream << "[";
      addDetails(stream);
      stream << "]";
    }

    if (recursive) {
      for (const auto &source : sources()) {
        stream << "\n";
        source->toStringInternal(stream, detailed, true, indentationSize + 2);
      }
    }
  }
};

using PlanNodePtr = std::shared_ptr<const PlanNode>;

class PlanNodeIdGenerator {
public:
  std::string next() { return std::to_string(nextId_++); }

private:
  int nextId_ = 0;
};

inline void appendCommaSeparated(std::stringstream &stream, const std::vector<std::string> &values) {
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      stream << ", ";
    }
    stream << values[i];
  }
}

// Nodes
class ValuesNode : public PlanNode {
public:
  using PlanNode::toString;
  ValuesNode(PlanNodeId id, std::vector<RowVectorPtr> values) : id_(std::move(id)), values_(std::move(values)) {}
  const PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "Values"; }
  std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {}; }
  const std::vector<RowVectorPtr> &values() const { return values_; }

private:
  void addDetails(std::stringstream &stream) const override {
    vector_size_t totalCount = 0;
    for (const auto &vector : values_) {
      if (vector) {
        totalCount += vector->size();
      }
    }
    stream << totalCount << " rows in " << values_.size() << " vectors";
  }

  PlanNodeId id_;
  std::vector<RowVectorPtr> values_;
};

class FilterNode : public PlanNode {
public:
  using PlanNode::toString;
  FilterNode(PlanNodeId id, PlanNodePtr source, TypedExprPtr filter)
      : id_(std::move(id)), source_(std::move(source)), filter_(std::move(filter)) {}
  const PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "Filter"; }
  std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {source_}; }
  const TypedExprPtr &filter() const { return filter_; }

private:
  void addDetails(std::stringstream &stream) const override {
    if (filter_) {
      stream << "expression: " << filter_->toString();
    }
  }

  PlanNodeId id_;
  PlanNodePtr source_;
  TypedExprPtr filter_;
};

// ... Aggregation, OrderBy, etc.
// For now I stub them to compile demo.

class AggregationNode : public PlanNode {
public:
  using PlanNode::toString;
  enum class Step { kSingle, kPartial, kIntermediate, kFinal };

  AggregationNode(PlanNodeId id, PlanNodePtr source, std::vector<std::string> groupingKeys,
                  std::vector<std::string> aggregates, Step step)
      : id_(std::move(id)), source_(std::move(source)), groupingKeys_(std::move(groupingKeys)),
        aggregates_(std::move(aggregates)), step_(step) {}
  const PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "Aggregation"; }
  std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {source_}; }
  const std::vector<std::string> &aggregates() const { return aggregates_; }
  const std::vector<std::string> &groupingKeys() const { return groupingKeys_; }
  Step step() const { return step_; }
  bool isPartial() const { return step_ == Step::kPartial || step_ == Step::kIntermediate; }

private:
  static std::string_view stepName(Step step) {
    switch (step) {
    case Step::kSingle:
      return "SINGLE";
    case Step::kPartial:
      return "PARTIAL";
    case Step::kIntermediate:
      return "INTERMEDIATE";
    case Step::kFinal:
      return "FINAL";
    }
    return "UNKNOWN";
  }

  void addDetails(std::stringstream &stream) const override {
    stream << stepName(step_);

    if (!groupingKeys_.empty()) {
      stream << " [";
      appendCommaSeparated(stream, groupingKeys_);
      stream << "]";
    }

    if (!aggregates_.empty()) {
      stream << " ";
      appendCommaSeparated(stream, aggregates_);
    }
  }

  PlanNodeId id_;
  PlanNodePtr source_;
  std::vector<std::string> groupingKeys_;
  std::vector<std::string> aggregates_;
  Step step_{Step::kSingle};
};

class OrderByNode : public PlanNode {
public:
  using PlanNode::toString;
  OrderByNode(PlanNodeId id, PlanNodePtr source, std::vector<std::string> keys, bool isPartial)
      : id_(std::move(id)), source_(std::move(source)), keys_(std::move(keys)), isPartial_(isPartial) {}
  const PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "OrderBy"; }
  std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {source_}; }
  const std::vector<std::string> &keys() const { return keys_; }
  bool isPartial() const { return isPartial_; }

private:
  void addDetails(std::stringstream &stream) const override {
    if (isPartial_) {
      stream << "PARTIAL ";
    }
    appendCommaSeparated(stream, keys_);
  }

  PlanNodeId id_;
  PlanNodePtr source_;
  std::vector<std::string> keys_;
  bool isPartial_{false};
};

class TopNNode : public PlanNode {
public:
  using PlanNode::toString;
  TopNNode(PlanNodeId id, PlanNodePtr source, int count, std::vector<std::string> keys, bool isPartial = false)
      : id_(std::move(id)), source_(std::move(source)), count_(count), keys_(std::move(keys)), isPartial_(isPartial) {}
  const PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "TopN"; }
  std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {source_}; }
  int count() const { return count_; }
  const std::vector<std::string> &keys() const { return keys_; }
  bool isPartial() const { return isPartial_; }

private:
  void addDetails(std::stringstream &stream) const override {
    if (isPartial_) {
      stream << "PARTIAL ";
    }
    stream << count_;
    if (!keys_.empty()) {
      stream << " ";
      appendCommaSeparated(stream, keys_);
    }
  }

  PlanNodeId id_;
  PlanNodePtr source_;
  int count_;
  std::vector<std::string> keys_;
  bool isPartial_{false};
};

class TableScanNode : public PlanNode {
public:
  using PlanNode::toString;
  TableScanNode(PlanNodeId id, tpch::Table table, std::vector<std::string> columns)
      : id_(std::move(id)), table_(table), columns_(std::move(columns)) {}
  const PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "TableScan"; }
  std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {}; }
  tpch::Table table() const { return table_; }
  const std::vector<std::string> &columns() const { return columns_; }

private:
  PlanNodeId id_;
  tpch::Table table_;
  std::vector<std::string> columns_;
};

class HashJoinNode : public PlanNode {
public:
  using PlanNode::toString;
  enum class JoinType { kInner, kLeft, kRight, kFull, kAnti };

  HashJoinNode(PlanNodeId id, PlanNodePtr probe, PlanNodePtr build)
      : id_(std::move(id)), probe_(std::move(probe)), build_(std::move(build)) {}

  HashJoinNode(PlanNodeId id, PlanNodePtr probe, PlanNodePtr build, std::vector<std::string> leftKeys,
               std::vector<std::string> rightKeys, std::string filter, std::vector<std::string> output,
               JoinType joinType = JoinType::kInner)
      : id_(std::move(id)), probe_(std::move(probe)), build_(std::move(build)), leftKeys_(std::move(leftKeys)),
        rightKeys_(std::move(rightKeys)), filter_(std::move(filter)), output_(std::move(output)), joinType_(joinType) {}

  const PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "HashJoin"; }
  std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {probe_, build_}; }
  const std::vector<std::string> &leftKeys() const { return leftKeys_; }
  const std::vector<std::string> &rightKeys() const { return rightKeys_; }
  const std::string &filter() const { return filter_; }
  const std::vector<std::string> &output() const { return output_; }
  JoinType joinType() const { return joinType_; }

private:
  static std::string_view joinTypeName(JoinType joinType) {
    switch (joinType) {
    case JoinType::kInner:
      return "INNER";
    case JoinType::kLeft:
      return "LEFT";
    case JoinType::kRight:
      return "RIGHT";
    case JoinType::kFull:
      return "FULL";
    case JoinType::kAnti:
      return "ANTI";
    }
    return "UNKNOWN";
  }

  void addDetails(std::stringstream &stream) const override {
    stream << joinTypeName(joinType_);

    const auto keysCount = std::min(leftKeys_.size(), rightKeys_.size());
    if (keysCount > 0) {
      stream << " ";
      for (size_t i = 0; i < keysCount; ++i) {
        if (i > 0) {
          stream << " AND ";
        }
        stream << leftKeys_[i] << "=" << rightKeys_[i];
      }
    }

    if (!filter_.empty()) {
      stream << ", filter: " << filter_;
    }
  }

  PlanNodeId id_;
  PlanNodePtr probe_;
  PlanNodePtr build_;
  std::vector<std::string> leftKeys_;
  std::vector<std::string> rightKeys_;
  std::string filter_;
  std::vector<std::string> output_;
  JoinType joinType_{JoinType::kInner};
};

class FileScanNode : public PlanNode {
public:
  using PlanNode::toString;
  FileScanNode(PlanNodeId id, RowTypePtr outputType, std::string path)
      : id_(std::move(id)), outputType_(std::move(outputType)), path_(std::move(path)) {}
  const PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "FileScan"; }
  std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {}; }
  const RowTypePtr &outputType() const { return outputType_; }
  const std::string &path() const { return path_; }

private:
  void addDetails(std::stringstream &stream) const override { stream << "path: " << path_; }

  PlanNodeId id_;
  RowTypePtr outputType_;
  std::string path_;
};

class TableWriteNode : public PlanNode {
public:
  using PlanNode::toString;
  TableWriteNode(PlanNodeId id, PlanNodePtr source, std::string path)
      : id_(std::move(id)), source_(std::move(source)), path_(std::move(path)) {}
  const PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "TableWrite"; }
  std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {source_}; }
  const std::string &path() const { return path_; }

private:
  void addDetails(std::stringstream &stream) const override { stream << "path: " << path_; }

  PlanNodeId id_;
  PlanNodePtr source_;
  std::string path_;
};

class LocalExchangeNode : public PlanNode {
public:
  using PlanNode::toString;
  explicit LocalExchangeNode(PlanNodeId id) : id_(std::move(id)) {}
  const PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "LocalExchange"; }
  std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {}; }

private:
  PlanNodeId id_;
};

class LocalPartitionNode : public PlanNode {
public:
  using PlanNode::toString;
  LocalPartitionNode(PlanNodeId id, PlanNodePtr source, std::string exchangeId)
      : id_(std::move(id)), source_(std::move(source)), exchangeId_(std::move(exchangeId)) {}
  const PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "LocalPartition"; }
  std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {source_}; }
  const std::string &exchangeId() const { return exchangeId_; }

private:
  void addDetails(std::stringstream &stream) const override { stream << "exchangeId: " << exchangeId_; }

  PlanNodeId id_;
  PlanNodePtr source_;
  std::string exchangeId_;
};

class LocalMergeNode : public PlanNode {
public:
  using PlanNode::toString;
  LocalMergeNode(PlanNodeId id, PlanNodePtr source, std::string exchangeId)
      : id_(std::move(id)), source_(std::move(source)), exchangeId_(std::move(exchangeId)) {}
  const PlanNodeId &id() const override { return id_; }
  std::string toString() const override { return "LocalMerge"; }
  std::vector<std::shared_ptr<const PlanNode>> sources() const override { return {source_}; }
  const std::string &exchangeId() const { return exchangeId_; }

private:
  void addDetails(std::stringstream &stream) const override { stream << "exchangeId: " << exchangeId_; }

  PlanNodeId id_;
  PlanNodePtr source_;
  std::string exchangeId_;
};

} // namespace facebook::velox::core
