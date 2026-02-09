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
T readPod(const std::string &data, size_t &offset) {
  VELOX_CHECK(offset + sizeof(T) <= data.size(), "FVX: Unexpected end of file");
  T value;
  std::memcpy(&value, data.data() + offset, sizeof(T));
  offset += sizeof(T);
  return value;
}

std::string readString(const std::string &data, size_t &offset) {
  uint32_t length = readPod<uint32_t>(data, offset);
  VELOX_CHECK(offset + length <= data.size(), "FVX: Unexpected end of string");
  std::string value(data.data() + offset, length);
  offset += length;
  return value;
}

class PrefixReader {
public:
  PrefixReader(const ReadFile &file, uint64_t fileSize) : file_(file), fileSize_(fileSize) {}

  template <typename T>
  T readPod() {
    ensure(sizeof(T));
    T value;
    std::memcpy(&value, buffer_.data() + offset_, sizeof(T));
    offset_ += sizeof(T);
    return value;
  }

  std::string readString() {
    uint32_t length = readPod<uint32_t>();
    ensure(length);
    std::string value(buffer_.data() + offset_, length);
    offset_ += length;
    return value;
  }

  void readBytes(char *out, size_t length) {
    ensure(length);
    std::memcpy(out, buffer_.data() + offset_, length);
    offset_ += length;
  }

private:
  void ensure(size_t length) {
    const auto required = offset_ + length;
    VELOX_CHECK(required <= fileSize_, "FVX: Unexpected end of file");
    while (buffer_.size() < required) {
      const auto readOffset = static_cast<uint64_t>(buffer_.size());
      const auto remaining = fileSize_ - readOffset;
      const auto toRead = std::min<uint64_t>(std::max<uint64_t>(kChunkSize, required - readOffset), remaining);
      auto chunk = file_.pread(readOffset, toRead);
      VELOX_CHECK(!chunk.empty(), "FVX: Unexpected end of file");
      buffer_.append(chunk);
    }
  }

  const ReadFile &file_;
  const uint64_t fileSize_;
  std::string buffer_;
  size_t offset_{0};
  static constexpr uint64_t kChunkSize = 64 * 1024;
};

int compareStrings(const std::string &left, const std::string &right) {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}

} // namespace

FvxReader::FvxReader(std::shared_ptr<ReadFile> file, memory::MemoryPool *pool) : file_(std::move(file)), pool_(pool) {
  VELOX_CHECK(file_ != nullptr, "ReadFile must not be null");
  parseFile();
  buildNameToIndex();
}

std::unique_ptr<FvxRowReader> FvxReader::createRowReader(const dwio::common::RowReaderOptions &options) const {
  return std::make_unique<FvxRowReader>(this, options);
}

void FvxReader::parseFile() {
  fileSize_ = file_->size();
  VELOX_CHECK(fileSize_ >= sizeof(kMagic), "FVX: file too small");
  PrefixReader metadataReader(*file_, fileSize_);
  char magic[sizeof(kMagic)];
  metadataReader.readBytes(magic, sizeof(magic));
  VELOX_CHECK(std::memcmp(magic, kMagic, sizeof(kMagic)) == 0, "FVX: invalid magic");

  uint32_t version = metadataReader.readPod<uint32_t>();
  VELOX_CHECK(version == kVersion, "FVX: unsupported version");

  uint32_t columnCount = metadataReader.readPod<uint32_t>();
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(columnCount);
  types.reserve(columnCount);
  for (uint32_t i = 0; i < columnCount; ++i) {
    auto name = metadataReader.readString();
    auto kind = static_cast<TypeKind>(metadataReader.readPod<uint8_t>());
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

  uint32_t rowGroupCount = metadataReader.readPod<uint32_t>();
  rowGroups_.reserve(rowGroupCount);
  for (uint32_t g = 0; g < rowGroupCount; ++g) {
    RowGroup group;
    group.rowCount = metadataReader.readPod<uint32_t>();
    group.columns.reserve(columnCount);
    for (uint32_t c = 0; c < columnCount; ++c) {
      ColumnChunk column;
      column.offset = metadataReader.readPod<uint64_t>();
      column.length = metadataReader.readPod<uint64_t>();
      VELOX_CHECK(column.offset <= fileSize_, "FVX: column offset out of bounds");
      VELOX_CHECK(column.length <= fileSize_ - column.offset, "FVX: column length out of bounds");
      auto kind = rowType_->childAt(c)->kind();
      column.stats.kind = kind;
      column.stats.hasMinMax = true;
      if (kind == TypeKind::BIGINT) {
        column.stats.minBigint = metadataReader.readPod<int64_t>();
        column.stats.maxBigint = metadataReader.readPod<int64_t>();
      } else if (kind == TypeKind::INTEGER) {
        column.stats.minInt = metadataReader.readPod<int32_t>();
        column.stats.maxInt = metadataReader.readPod<int32_t>();
      } else if (kind == TypeKind::VARCHAR) {
        column.stats.minString = metadataReader.readString();
        column.stats.maxString = metadataReader.readString();
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

FvxRowReader::FvxRowReader(const FvxReader *reader, dwio::common::RowReaderOptions options)
    : reader_(reader), options_(std::move(options)) {
  buildProjection();
  buildFiltersAndRequiredColumns();
}

bool FvxRowReader::next(size_t batchSize, RowVectorPtr &out) {
  if (batchSize == 0) {
    return false;
  }

  while (true) {
    if (!currentGroup_.has_value()) {
      if (!loadNextMatchingRowGroup()) {
        return false;
      }
    }

    auto &cache = currentGroup_.value();
    if (rowOffsetInGroup_ >= static_cast<vector_size_t>(cache.selectedRows.size())) {
      currentGroup_.reset();
      rowOffsetInGroup_ = 0;
      continue;
    }

    vector_size_t remaining = static_cast<vector_size_t>(cache.selectedRows.size()) - rowOffsetInGroup_;
    vector_size_t count = static_cast<vector_size_t>(std::min<size_t>(batchSize, remaining));
    out = buildRowVectorFromCache(cache, cache.selectedRows.data() + rowOffsetInGroup_, count);
    rowOffsetInGroup_ += count;
    return true;
  }
}

bool FvxRowReader::loadNextMatchingRowGroup() {
  while (rowGroupIndex_ < reader_->rowGroups_.size()) {
    const auto &rowGroup = reader_->rowGroups_[rowGroupIndex_];
    ++rowGroupIndex_;
    if (!rowGroupMatches(rowGroup)) {
      continue;
    }

    RowGroupCache cache;
    cache.rowCount = rowGroup.rowCount;
    cache.columns.resize(reader_->rowType_->size());
    cache.selectedRows.reserve(rowGroup.rowCount);

    if (filters_.empty()) {
      for (vector_size_t row = 0; row < static_cast<vector_size_t>(rowGroup.rowCount); ++row) {
        cache.selectedRows.push_back(row);
      }
    } else {
      // Decode only filter columns first, then evaluate row-level filters.
      for (auto columnIndex : filterColumnIndices_) {
        auto kind = reader_->rowType_->childAt(columnIndex)->kind();
        cache.columns[columnIndex] = decodeColumn(rowGroup.columns[columnIndex], kind, rowGroup.rowCount);
      }

      for (vector_size_t row = 0; row < static_cast<vector_size_t>(rowGroup.rowCount); ++row) {
        if (rowMatchesFilters(cache, row)) {
          cache.selectedRows.push_back(row);
        }
      }
    }

    if (cache.selectedRows.empty()) {
      continue;
    }

    // Decode projected columns only for row groups with at least one matching row.
    // Reuse already decoded filter columns when projection overlaps filter columns.
    for (auto columnIndex : projectedIndices_) {
      if (cache.columns[columnIndex].has_value()) {
        continue;
      }
      auto kind = reader_->rowType_->childAt(columnIndex)->kind();
      cache.columns[columnIndex] = decodeColumn(rowGroup.columns[columnIndex], kind, rowGroup.rowCount);
    }

    currentGroup_ = std::move(cache);
    rowOffsetInGroup_ = 0;
    return true;
  }
  return false;
}

bool FvxRowReader::rowGroupMatches(const FvxReader::RowGroup &rowGroup) const {
  for (const auto &filter : filters_) {
    const auto &stats = rowGroup.columns[filter.columnIndex].stats;
    if (!columnMayMatch(stats, filter.op, filter.value)) {
      return false;
    }
  }
  return true;
}

bool FvxRowReader::columnMayMatch(const FvxReader::ColumnStats &stats, dwio::common::CompareOp op,
                                  const Variant &value) const {
  if (!stats.hasMinMax || value.isNull()) {
    return true;
  }
  if (stats.kind == TypeKind::BIGINT || stats.kind == TypeKind::INTEGER) {
    if (value.kind() != TypeKind::BIGINT && value.kind() != TypeKind::INTEGER) {
      return true;
    }
    int64_t v = value.value<int64_t>();
    int64_t minValue = (stats.kind == TypeKind::BIGINT) ? stats.minBigint : stats.minInt;
    int64_t maxValue = (stats.kind == TypeKind::BIGINT) ? stats.maxBigint : stats.maxInt;
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
      return !(stats.minString == stats.maxString && stats.minString == v);
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

bool FvxRowReader::rowMatchesFilters(const RowGroupCache &cache, vector_size_t row) const {
  for (const auto &filter : filters_) {
    const auto &column = getColumnBuffer(cache, filter.columnIndex);
    if (!columnMatches(column, row, filter.op, filter.value)) {
      return false;
    }
  }
  return true;
}

bool FvxRowReader::columnMatches(const ColumnBuffer &column, vector_size_t row, dwio::common::CompareOp op,
                                 const Variant &value) const {
  if (value.isNull()) {
    return false;
  }

  if (column.kind == TypeKind::BIGINT || column.kind == TypeKind::INTEGER) {
    if (value.kind() != TypeKind::BIGINT && value.kind() != TypeKind::INTEGER) {
      return false;
    }

    int64_t rowValue = (column.kind == TypeKind::BIGINT) ? column.int64s[row] : column.int32s[row];
    int64_t filterValue = value.value<int64_t>();
    switch (op) {
    case dwio::common::CompareOp::EQ:
      return rowValue == filterValue;
    case dwio::common::CompareOp::NE:
      return rowValue != filterValue;
    case dwio::common::CompareOp::LT:
      return rowValue < filterValue;
    case dwio::common::CompareOp::LE:
      return rowValue <= filterValue;
    case dwio::common::CompareOp::GT:
      return rowValue > filterValue;
    case dwio::common::CompareOp::GE:
      return rowValue >= filterValue;
    }
  }

  if (column.kind == TypeKind::VARCHAR) {
    if (value.kind() != TypeKind::VARCHAR) {
      return false;
    }

    const std::string &rowValue = column.strings[row];
    const std::string filterValue = value.value<std::string>();
    int cmp = compareStrings(rowValue, filterValue);
    switch (op) {
    case dwio::common::CompareOp::EQ:
      return cmp == 0;
    case dwio::common::CompareOp::NE:
      return cmp != 0;
    case dwio::common::CompareOp::LT:
      return cmp < 0;
    case dwio::common::CompareOp::LE:
      return cmp <= 0;
    case dwio::common::CompareOp::GT:
      return cmp > 0;
    case dwio::common::CompareOp::GE:
      return cmp >= 0;
    }
  }

  VELOX_FAIL("FVX supports BIGINT, INTEGER, VARCHAR only");
  return false;
}

const FvxRowReader::ColumnBuffer &FvxRowReader::getColumnBuffer(const RowGroupCache &cache, size_t columnIndex) const {
  VELOX_CHECK(columnIndex < cache.columns.size(), "FVX: invalid column index {}", columnIndex);
  VELOX_CHECK(cache.columns[columnIndex].has_value(), "FVX: column {} was not decoded", columnIndex);
  return cache.columns[columnIndex].value();
}

RowVectorPtr FvxRowReader::buildRowVectorFromCache(const RowGroupCache &cache, const vector_size_t *rows,
                                                   vector_size_t count) const {
  std::vector<VectorPtr> children;
  children.reserve(projectedIndices_.size());

  for (size_t i = 0; i < projectedIndices_.size(); ++i) {
    const auto columnIndex = projectedIndices_[i];
    const auto &column = getColumnBuffer(cache, columnIndex);
    auto type = outputType_->childAt(i);
    if (column.kind == TypeKind::BIGINT) {
      auto buffer = AlignedBuffer::allocate(count * sizeof(int64_t), reader_->pool_);
      auto *rawValues = buffer->asMutable<int64_t>();
      for (vector_size_t row = 0; row < count; ++row) {
        rawValues[row] = column.int64s[rows[row]];
      }
      children.push_back(std::make_shared<FlatVector<int64_t>>(reader_->pool_, type, nullptr, count, buffer));
    } else if (column.kind == TypeKind::INTEGER) {
      auto buffer = AlignedBuffer::allocate(count * sizeof(int32_t), reader_->pool_);
      auto *rawValues = buffer->asMutable<int32_t>();
      for (vector_size_t row = 0; row < count; ++row) {
        rawValues[row] = column.int32s[rows[row]];
      }
      children.push_back(std::make_shared<FlatVector<int32_t>>(reader_->pool_, type, nullptr, count, buffer));
    } else if (column.kind == TypeKind::VARCHAR) {
      size_t totalSize = 0;
      for (vector_size_t row = 0; row < count; ++row) {
        totalSize += column.strings[rows[row]].size();
      }
      auto values = AlignedBuffer::allocate(count * sizeof(StringView), reader_->pool_);
      auto *rawValues = values->asMutable<StringView>();
      auto dataBuffer = AlignedBuffer::allocate(totalSize, reader_->pool_);
      char *cursor = dataBuffer->asMutable<char>();
      size_t bytesOffset = 0;
      for (vector_size_t row = 0; row < count; ++row) {
        const auto &value = column.strings[rows[row]];
        std::memcpy(cursor + bytesOffset, value.data(), value.size());
        rawValues[row] = StringView(cursor + bytesOffset, value.size());
        bytesOffset += value.size();
      }
      auto vector = std::make_shared<FlatVector<StringView>>(reader_->pool_, type, nullptr, count, values);
      vector->addStringBuffer(dataBuffer);
      children.push_back(vector);
    } else {
      VELOX_FAIL("FVX supports BIGINT, INTEGER, VARCHAR only");
    }
  }

  return std::make_shared<RowVector>(reader_->pool_, outputType_, nullptr, count, std::move(children));
}

FvxRowReader::ColumnBuffer FvxRowReader::decodeColumn(const FvxReader::ColumnChunk &chunk, TypeKind kind,
                                                      uint32_t rowCount) const {
  ColumnBuffer buffer;
  buffer.kind = kind;
  VELOX_CHECK(chunk.offset <= reader_->fileSize_, "FVX: column offset out of bounds");
  VELOX_CHECK(chunk.length <= reader_->fileSize_ - chunk.offset, "FVX: column length out of bounds");

  auto chunkData = reader_->file_->pread(chunk.offset, chunk.length);
  size_t offset = 0;
  if (kind == TypeKind::BIGINT) {
    buffer.int64s.resize(rowCount);
    const auto byteSize = static_cast<size_t>(rowCount) * sizeof(int64_t);
    VELOX_CHECK(offset + byteSize <= chunkData.size(), "FVX: bigint column out of bounds");
    std::memcpy(buffer.int64s.data(), chunkData.data() + offset, byteSize);
  } else if (kind == TypeKind::INTEGER) {
    buffer.int32s.resize(rowCount);
    const auto byteSize = static_cast<size_t>(rowCount) * sizeof(int32_t);
    VELOX_CHECK(offset + byteSize <= chunkData.size(), "FVX: integer column out of bounds");
    std::memcpy(buffer.int32s.data(), chunkData.data() + offset, byteSize);
  } else if (kind == TypeKind::VARCHAR) {
    uint32_t totalBytes = readPod<uint32_t>(chunkData, offset);
    std::vector<uint32_t> offsets(rowCount + 1);
    VELOX_CHECK(offset + offsets.size() * sizeof(uint32_t) + totalBytes <= chunkData.size(),
                "FVX: varchar column out of bounds");
    std::memcpy(offsets.data(), chunkData.data() + offset, offsets.size() * sizeof(uint32_t));
    offset += offsets.size() * sizeof(uint32_t);
    buffer.strings.reserve(rowCount);
    const char *base = chunkData.data() + offset;
    for (uint32_t i = 0; i < rowCount; ++i) {
      uint32_t start = offsets[i];
      uint32_t end = offsets[i + 1];
      VELOX_CHECK(end >= start, "FVX: invalid string offsets");
      buffer.strings.emplace_back(base + start, end - start);
    }
    VELOX_CHECK(offsets.back() == totalBytes, "FVX: string data length mismatch");
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
  for (const auto &name : options_.projectedColumns()) {
    auto it = reader_->nameToIndex_.find(name);
    VELOX_CHECK(it != reader_->nameToIndex_.end(), "Unknown column {}", name);
    projectedIndices_.push_back(it->second);
    names.push_back(name);
    types.push_back(reader_->rowType_->childAt(it->second));
  }
  outputType_ = ROW(std::move(names), std::move(types));
}

void FvxRowReader::buildFiltersAndRequiredColumns() {
  filters_.clear();
  filterColumnIndices_.clear();
  filters_.reserve(options_.filters().size());
  for (const auto &filter : options_.filters()) {
    auto it = reader_->nameToIndex_.find(filter.column);
    VELOX_CHECK(it != reader_->nameToIndex_.end(), "Unknown filter column {}", filter.column);
    filters_.push_back(PreparedFilter{it->second, filter.op, filter.value});
    filterColumnIndices_.push_back(it->second);
  }

  std::sort(filterColumnIndices_.begin(), filterColumnIndices_.end());
  filterColumnIndices_.erase(std::unique(filterColumnIndices_.begin(), filterColumnIndices_.end()),
                             filterColumnIndices_.end());
}

} // namespace facebook::velox::dwio::fvx
