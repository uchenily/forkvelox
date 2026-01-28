#include "velox/dwio/fvx/FvxReader.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::dwio::fvx {
namespace {

constexpr char kMagic[] = {'F', 'V', 'X', '1'};
constexpr uint32_t kVersion = 1;

template <typename T>
T readPod(const std::string& data, size_t& offset) {
  VELOX_CHECK(
      offset + sizeof(T) <= data.size(),
      "FVX: Unexpected end of file");
  T value;
  std::memcpy(&value, data.data() + offset, sizeof(T));
  offset += sizeof(T);
  return value;
}

std::string readString(const std::string& data, size_t& offset) {
  uint32_t length = readPod<uint32_t>(data, offset);
  VELOX_CHECK(
      offset + length <= data.size(),
      "FVX: Unexpected end of string");
  std::string value(data.data() + offset, length);
  offset += length;
  return value;
}

int compareStrings(const std::string& left, const std::string& right) {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}

} // namespace

FvxReader::FvxReader(std::shared_ptr<ReadFile> file, memory::MemoryPool* pool)
    : file_(std::move(file)), pool_(pool) {
  VELOX_CHECK(file_ != nullptr, "ReadFile must not be null");
  parseFile();
  buildNameToIndex();
}

std::unique_ptr<FvxRowReader> FvxReader::createRowReader(
    const dwio::common::RowReaderOptions& options) const {
  return std::make_unique<FvxRowReader>(this, options);
}

void FvxReader::parseFile() {
  auto size = file_->size();
  data_ = file_->pread(0, size);
  size_t offset = 0;
  VELOX_CHECK(
      data_.size() >= sizeof(kMagic),
      "FVX: file too small");
  VELOX_CHECK(
      std::memcmp(data_.data(), kMagic, sizeof(kMagic)) == 0,
      "FVX: invalid magic");
  offset += sizeof(kMagic);
  uint32_t version = readPod<uint32_t>(data_, offset);
  VELOX_CHECK(version == kVersion, "FVX: unsupported version");

  uint32_t columnCount = readPod<uint32_t>(data_, offset);
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(columnCount);
  types.reserve(columnCount);
  for (uint32_t i = 0; i < columnCount; ++i) {
    auto name = readString(data_, offset);
    auto kind = static_cast<TypeKind>(readPod<uint8_t>(data_, offset));
    names.push_back(name);
    if (kind == TypeKind::BIGINT) {
      types.push_back(BIGINT());
    } else if (kind == TypeKind::INTEGER) {
      types.push_back(INTEGER());
    } else if (kind == TypeKind::VARCHAR) {
      types.push_back(VARCHAR());
    } else {
      VELOX_FAIL("FVX: unsupported type kind");
    }
  }
  rowType_ = ROW(std::move(names), std::move(types));

  uint32_t rowGroupCount = readPod<uint32_t>(data_, offset);
  rowGroups_.reserve(rowGroupCount);
  for (uint32_t g = 0; g < rowGroupCount; ++g) {
    RowGroup group;
    group.rowCount = readPod<uint32_t>(data_, offset);
    group.columns.reserve(columnCount);
    for (uint32_t c = 0; c < columnCount; ++c) {
      ColumnChunk column;
      column.offset = readPod<uint64_t>(data_, offset);
      column.length = readPod<uint64_t>(data_, offset);
      auto kind = rowType_->childAt(c)->kind();
      column.stats.kind = kind;
      column.stats.hasMinMax = true;
      if (kind == TypeKind::BIGINT) {
        column.stats.minBigint = readPod<int64_t>(data_, offset);
        column.stats.maxBigint = readPod<int64_t>(data_, offset);
      } else if (kind == TypeKind::INTEGER) {
        column.stats.minInt = readPod<int32_t>(data_, offset);
        column.stats.maxInt = readPod<int32_t>(data_, offset);
      } else if (kind == TypeKind::VARCHAR) {
        column.stats.minString = readString(data_, offset);
        column.stats.maxString = readString(data_, offset);
      } else {
        VELOX_FAIL("FVX: unsupported type kind");
      }
      group.columns.push_back(std::move(column));
    }
    rowGroups_.push_back(std::move(group));
  }
}

void FvxReader::buildNameToIndex() {
  for (size_t i = 0; i < rowType_->size(); ++i) {
    nameToIndex_[rowType_->nameOf(i)] = i;
  }
}

FvxRowReader::FvxRowReader(
    const FvxReader* reader,
    dwio::common::RowReaderOptions options)
    : reader_(reader), options_(std::move(options)) {
  buildProjection();
}

bool FvxRowReader::next(size_t batchSize, RowVectorPtr& out) {
  if (!currentGroup_.has_value()) {
    if (!loadNextMatchingRowGroup()) {
      return false;
    }
  }

  auto& cache = currentGroup_.value();
  if (rowOffsetInGroup_ >= static_cast<vector_size_t>(cache.rowCount)) {
    currentGroup_.reset();
    rowOffsetInGroup_ = 0;
    return next(batchSize, out);
  }

  vector_size_t remaining =
      static_cast<vector_size_t>(cache.rowCount) - rowOffsetInGroup_;
  vector_size_t count =
      static_cast<vector_size_t>(std::min<size_t>(batchSize, remaining));
  out = buildRowVectorFromCache(cache, rowOffsetInGroup_, count);
  rowOffsetInGroup_ += count;
  return true;
}

bool FvxRowReader::loadNextMatchingRowGroup() {
  while (rowGroupIndex_ < reader_->rowGroups_.size()) {
    const auto& rowGroup = reader_->rowGroups_[rowGroupIndex_];
    ++rowGroupIndex_;
    if (!rowGroupMatches(rowGroup)) {
      continue;
    }
    RowGroupCache cache;
    cache.rowCount = rowGroup.rowCount;
    cache.columns.reserve(projectedIndices_.size());
    for (size_t i = 0; i < projectedIndices_.size(); ++i) {
      auto columnIndex = projectedIndices_[i];
      auto kind = reader_->rowType_->childAt(columnIndex)->kind();
      cache.columns.push_back(
          decodeColumn(rowGroup.columns[columnIndex], kind, rowGroup.rowCount));
    }
    currentGroup_ = std::move(cache);
    rowOffsetInGroup_ = 0;
    return true;
  }
  return false;
}

bool FvxRowReader::rowGroupMatches(
    const FvxReader::RowGroup& rowGroup) const {
  for (const auto& filter : options_.filters()) {
    auto it = reader_->nameToIndex_.find(filter.column);
    if (it == reader_->nameToIndex_.end()) {
      continue;
    }
    const auto& stats = rowGroup.columns[it->second].stats;
    if (!columnMayMatch(stats, filter.op, filter.value)) {
      return false;
    }
  }
  return true;
}

bool FvxRowReader::columnMayMatch(
    const FvxReader::ColumnStats& stats,
    dwio::common::CompareOp op,
    const Variant& value) const {
  if (!stats.hasMinMax || value.isNull()) {
    return true;
  }
  if (stats.kind == TypeKind::BIGINT || stats.kind == TypeKind::INTEGER) {
    if (value.kind() != TypeKind::BIGINT && value.kind() != TypeKind::INTEGER) {
      return true;
    }
    int64_t v = value.value<int64_t>();
    int64_t minValue =
        (stats.kind == TypeKind::BIGINT) ? stats.minBigint : stats.minInt;
    int64_t maxValue =
        (stats.kind == TypeKind::BIGINT) ? stats.maxBigint : stats.maxInt;
    switch (op) {
      case dwio::common::CompareOp::EQ:
        return !(v < minValue || v > maxValue);
      case dwio::common::CompareOp::NE:
        return !(minValue == maxValue && v == minValue);
      case dwio::common::CompareOp::LT:
        return minValue < v;
      case dwio::common::CompareOp::LE:
        return minValue <= v;
      case dwio::common::CompareOp::GT:
        return maxValue > v;
      case dwio::common::CompareOp::GE:
        return maxValue >= v;
    }
  }

  if (stats.kind == TypeKind::VARCHAR) {
    if (value.kind() != TypeKind::VARCHAR) {
      return true;
    }
    std::string v = value.value<std::string>();
    int minCmp = compareStrings(stats.minString, v);
    int maxCmp = compareStrings(stats.maxString, v);
    switch (op) {
      case dwio::common::CompareOp::EQ:
        return !(minCmp > 0 || maxCmp < 0);
      case dwio::common::CompareOp::NE:
        return !(stats.minString == stats.maxString &&
                 stats.minString == v);
      case dwio::common::CompareOp::LT:
        return compareStrings(stats.minString, v) < 0;
      case dwio::common::CompareOp::LE:
        return compareStrings(stats.minString, v) <= 0;
      case dwio::common::CompareOp::GT:
        return compareStrings(stats.maxString, v) > 0;
      case dwio::common::CompareOp::GE:
        return compareStrings(stats.maxString, v) >= 0;
    }
  }
  return true;
}

RowVectorPtr FvxRowReader::buildRowVectorFromCache(
    const RowGroupCache& cache,
    vector_size_t offset,
    vector_size_t count) const {
  std::vector<VectorPtr> children;
  children.reserve(cache.columns.size());

  for (size_t i = 0; i < cache.columns.size(); ++i) {
    const auto& column = cache.columns[i];
    auto type = outputType_->childAt(i);
    if (column.kind == TypeKind::BIGINT) {
      auto buffer =
          AlignedBuffer::allocate(count * sizeof(int64_t), reader_->pool_);
      std::memcpy(
          buffer->as_mutable_uint8_t(),
          column.int64s.data() + offset,
          count * sizeof(int64_t));
      children.push_back(std::make_shared<FlatVector<int64_t>>(
          reader_->pool_, type, nullptr, count, buffer));
    } else if (column.kind == TypeKind::INTEGER) {
      auto buffer =
          AlignedBuffer::allocate(count * sizeof(int32_t), reader_->pool_);
      std::memcpy(
          buffer->as_mutable_uint8_t(),
          column.int32s.data() + offset,
          count * sizeof(int32_t));
      children.push_back(std::make_shared<FlatVector<int32_t>>(
          reader_->pool_, type, nullptr, count, buffer));
    } else if (column.kind == TypeKind::VARCHAR) {
      const auto begin = column.strings.begin() + offset;
      const auto end = begin + count;
      size_t totalSize = 0;
      for (auto it = begin; it != end; ++it) {
        totalSize += it->size();
      }
      auto values =
          AlignedBuffer::allocate(count * sizeof(StringView), reader_->pool_);
      auto* rawValues = values->asMutable<StringView>();
      auto dataBuffer = AlignedBuffer::allocate(totalSize, reader_->pool_);
      char* cursor = dataBuffer->asMutable<char>();
      size_t bytesOffset = 0;
      size_t row = 0;
      for (auto it = begin; it != end; ++it, ++row) {
        std::memcpy(cursor + bytesOffset, it->data(), it->size());
        rawValues[row] = StringView(cursor + bytesOffset, it->size());
        bytesOffset += it->size();
      }
      auto vector = std::make_shared<FlatVector<StringView>>(
          reader_->pool_, type, nullptr, count, values);
      vector->addStringBuffer(dataBuffer);
      children.push_back(vector);
    } else {
      VELOX_FAIL("FVX supports BIGINT, INTEGER, VARCHAR only");
    }
  }

  return std::make_shared<RowVector>(
      reader_->pool_, outputType_, nullptr, count, std::move(children));
}

FvxRowReader::ColumnBuffer FvxRowReader::decodeColumn(
    const FvxReader::ColumnChunk& chunk,
    TypeKind kind,
    uint32_t rowCount) const {
  ColumnBuffer buffer;
  buffer.kind = kind;
  size_t offset = static_cast<size_t>(chunk.offset);
  if (kind == TypeKind::BIGINT) {
    buffer.int64s.resize(rowCount);
    VELOX_CHECK(
        offset + rowCount * sizeof(int64_t) <= reader_->data_.size(),
        "FVX: bigint column out of bounds");
    std::memcpy(
        buffer.int64s.data(),
        reader_->data_.data() + offset,
        rowCount * sizeof(int64_t));
  } else if (kind == TypeKind::INTEGER) {
    buffer.int32s.resize(rowCount);
    VELOX_CHECK(
        offset + rowCount * sizeof(int32_t) <= reader_->data_.size(),
        "FVX: integer column out of bounds");
    std::memcpy(
        buffer.int32s.data(),
        reader_->data_.data() + offset,
        rowCount * sizeof(int32_t));
  } else if (kind == TypeKind::VARCHAR) {
    uint32_t totalBytes = readPod<uint32_t>(reader_->data_, offset);
    std::vector<uint32_t> offsets(rowCount + 1);
    VELOX_CHECK(
        offset + offsets.size() * sizeof(uint32_t) + totalBytes <=
            reader_->data_.size(),
        "FVX: varchar column out of bounds");
    std::memcpy(
        offsets.data(),
        reader_->data_.data() + offset,
        offsets.size() * sizeof(uint32_t));
    offset += offsets.size() * sizeof(uint32_t);
    buffer.strings.reserve(rowCount);
    const char* base = reader_->data_.data() + offset;
    for (uint32_t i = 0; i < rowCount; ++i) {
      uint32_t start = offsets[i];
      uint32_t end = offsets[i + 1];
      VELOX_CHECK(end >= start, "FVX: invalid string offsets");
      buffer.strings.emplace_back(base + start, end - start);
    }
    VELOX_CHECK(
        offsets.back() == totalBytes,
        "FVX: string data length mismatch");
  } else {
    VELOX_FAIL("FVX supports BIGINT, INTEGER, VARCHAR only");
  }
  return buffer;
}

void FvxRowReader::buildProjection() {
  if (!options_.hasProjection()) {
    projectedIndices_.resize(reader_->rowType_->size());
    for (size_t i = 0; i < projectedIndices_.size(); ++i) {
      projectedIndices_[i] = i;
    }
    outputType_ = reader_->rowType_;
    return;
  }

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (const auto& name : options_.projectedColumns()) {
    auto it = reader_->nameToIndex_.find(name);
    VELOX_CHECK(it != reader_->nameToIndex_.end(), "Unknown column {}", name);
    projectedIndices_.push_back(it->second);
    names.push_back(name);
    types.push_back(reader_->rowType_->childAt(it->second));
  }
  outputType_ = ROW(std::move(names), std::move(types));
}

} // namespace facebook::velox::dwio::fvx
