#include "velox/dwio/fvx/FvxReader.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::dwio::fvx {
namespace {

constexpr char kMagic[] = {'F', 'V', 'X', '3'};
constexpr uint32_t kVersion = 3;

template <typename T>
T readPod(std::string_view data, size_t &offset) {
  VELOX_CHECK(offset + sizeof(T) <= data.size(), "FVX: Unexpected end of file");
  T value;
  std::memcpy(&value, data.data() + offset, sizeof(T));
  offset += sizeof(T);
  return value;
}

std::string readString(std::string_view data, size_t &offset) {
  const uint32_t length = readPod<uint32_t>(data, offset);
  VELOX_CHECK(offset + length <= data.size(), "FVX: Unexpected end of string");
  std::string value(data.data() + offset, length);
  offset += length;
  return value;
}

struct PageHeader {
  uint8_t pageType{0};
  uint8_t encoding{0};
  uint32_t rowCount{0};
  uint32_t uncompressedSize{0};
  uint32_t compressedSize{0};
};

struct ParsedPage {
  PageHeader header;
  size_t payloadOffset{0};
};

ParsedPage parsePageHeader(std::string_view pageData) {
  size_t offset = 0;
  const auto headerSize = readPod<uint32_t>(pageData, offset);
  const size_t headerStart = offset;
  const size_t headerEnd = headerStart + headerSize;
  VELOX_CHECK(headerEnd <= pageData.size(), "FVX: page header out of bounds");

  size_t headerOffset = headerStart;
  PageHeader header;
  header.pageType = readPod<uint8_t>(pageData, headerOffset);
  header.encoding = readPod<uint8_t>(pageData, headerOffset);
  header.rowCount = readPod<uint32_t>(pageData, headerOffset);
  header.uncompressedSize = readPod<uint32_t>(pageData, headerOffset);
  header.compressedSize = readPod<uint32_t>(pageData, headerOffset);
  VELOX_CHECK(headerOffset == headerEnd, "FVX: invalid page header");
  VELOX_CHECK(headerEnd + header.compressedSize <= pageData.size(), "FVX: page payload out of bounds");
  return ParsedPage{header, headerEnd};
}

template <typename TStats>
void readStats(TStats &stats, TypeKind kind, std::string_view data, size_t &offset) {
  stats.kind = kind;
  stats.hasMinMax = true;
  if (kind == TypeKind::BIGINT) {
    stats.minBigint = readPod<int64_t>(data, offset);
    stats.maxBigint = readPod<int64_t>(data, offset);
  } else if (kind == TypeKind::INTEGER) {
    stats.minInt = readPod<int32_t>(data, offset);
    stats.maxInt = readPod<int32_t>(data, offset);
  } else if (kind == TypeKind::VARCHAR) {
    stats.minString = readString(data, offset);
    stats.maxString = readString(data, offset);
  } else {
    VELOX_FAIL("FVX: unsupported type kind");
  }
}

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
  constexpr uint64_t kTrailerSize = sizeof(uint32_t) + sizeof(kMagic);
  VELOX_CHECK(fileSize_ >= sizeof(kMagic) + kTrailerSize, "FVX: file too small");

  // 核心步骤1：校验前后 magic，并从文件尾部 trailer 定位 footer。
  auto prefixMagic = file_->pread(0, sizeof(kMagic));
  VELOX_CHECK(std::memcmp(prefixMagic.data(), kMagic, sizeof(kMagic)) == 0, "FVX: invalid prefix magic");

  const auto trailerOffset = fileSize_ - kTrailerSize;
  auto trailer = file_->pread(trailerOffset, kTrailerSize);
  size_t trailerReadOffset = 0;
  const auto footerSize = readPod<uint32_t>(trailer, trailerReadOffset);
  VELOX_CHECK(std::memcmp(trailer.data() + trailerReadOffset, kMagic, sizeof(kMagic)) == 0, "FVX: invalid suffix magic");

  VELOX_CHECK(footerSize <= trailerOffset, "FVX: invalid footer size");
  const auto footerOffset = trailerOffset - footerSize;
  VELOX_CHECK(footerOffset >= sizeof(kMagic), "FVX: footer overlaps prefix magic");

  auto footer = file_->pread(footerOffset, footerSize);
  size_t offset = 0;
  uint32_t version = readPod<uint32_t>(footer, offset);
  VELOX_CHECK(version == kVersion, "FVX: unsupported version");

  // 核心步骤2：解析 schema，构建 RowType。
  uint32_t columnCount = readPod<uint32_t>(footer, offset);
  VELOX_CHECK(columnCount > 0, "FVX: empty schema");
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(columnCount);
  types.reserve(columnCount);
  for (uint32_t i = 0; i < columnCount; ++i) {
    auto name = readString(footer, offset);
    auto kind = static_cast<TypeKind>(readPod<uint8_t>(footer, offset));
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

  // 核心步骤3：解析 row group / chunk / page 元数据并做一致性校验。
  uint32_t rowGroupCount = readPod<uint32_t>(footer, offset);
  rowGroups_.reserve(rowGroupCount);
  for (uint32_t g = 0; g < rowGroupCount; ++g) {
    RowGroup group;
    group.rowCount = readPod<uint32_t>(footer, offset);
    group.columns.reserve(columnCount);
    for (uint32_t c = 0; c < columnCount; ++c) {
      ColumnChunk column;
      column.offset = readPod<uint64_t>(footer, offset);
      column.length = readPod<uint64_t>(footer, offset);
      VELOX_CHECK(column.offset <= footerOffset, "FVX: column offset out of bounds");
      VELOX_CHECK(column.length <= footerOffset - column.offset, "FVX: column length out of bounds");
      auto kind = rowType_->childAt(c)->kind();
      readStats(column.stats, kind, footer, offset);

      uint32_t pageCount = readPod<uint32_t>(footer, offset);
      VELOX_CHECK(pageCount > 0, "FVX: empty page list");
      column.pages.reserve(pageCount);
      uint32_t totalPageRows = 0;
      uint64_t expectedPageOffset = column.offset;
      for (uint32_t p = 0; p < pageCount; ++p) {
        FvxReader::ColumnChunk::Page page;
        page.startRow = readPod<uint32_t>(footer, offset);
        page.rowCount = readPod<uint32_t>(footer, offset);
        VELOX_CHECK(page.rowCount > 0, "FVX: empty page");
        VELOX_CHECK(page.startRow == totalPageRows, "FVX: non-contiguous page row ranges");
        totalPageRows += page.rowCount;
        page.offset = readPod<uint64_t>(footer, offset);
        page.length = readPod<uint64_t>(footer, offset);
        page.uncompressedSize = readPod<uint32_t>(footer, offset);
        page.compressedSize = readPod<uint32_t>(footer, offset);
        VELOX_CHECK(page.offset == expectedPageOffset, "FVX: non-contiguous page layout");
        VELOX_CHECK(page.length <= footerOffset - page.offset, "FVX: page length out of bounds");
        expectedPageOffset += page.length;
        readStats(page.stats, kind, footer, offset);
        column.pages.push_back(std::move(page));
      }
      VELOX_CHECK(totalPageRows == group.rowCount, "FVX: page row count mismatch");
      VELOX_CHECK(expectedPageOffset == column.offset + column.length, "FVX: page length mismatch");
      group.columns.push_back(std::move(column));
    }
    rowGroups_.push_back(std::move(group));
  }
  VELOX_CHECK(offset == footer.size(), "FVX: trailing bytes in footer");
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

  // 按 batch 读取：当前行组耗尽后，继续加载下一个命中的行组。
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
    // 行组切换时清空页缓存，避免跨组误复用。
    clearPageDataCache();
    const auto &rowGroup = reader_->rowGroups_[rowGroupIndex_];
    ++rowGroupIndex_;
    // 先做 row group 级统计裁剪，不命中直接跳过整组。
    if (!rowGroupMatches(rowGroup)) {
      continue;
    }

    RowGroupCache cache;
    cache.rowGroup = &rowGroup;

    if (filters_.empty()) {
      // 无过滤条件时，整组行都进入候选集。
      cache.selectedRows.reserve(rowGroup.rowCount);
      for (vector_size_t row = 0; row < static_cast<vector_size_t>(rowGroup.rowCount); ++row) {
        cache.selectedRows.push_back(row);
      }
    } else {
      // 有过滤时，候选行从全量开始并按过滤列逐步收缩。
      std::vector<vector_size_t> candidateRows(rowGroup.rowCount);
      std::iota(candidateRows.begin(), candidateRows.end(), 0);
      for (auto columnIndex : filterColumnIndices_) {
        const auto &chunk = rowGroup.columns[columnIndex];
        // 先用 page 级统计过滤掉不可能命中的页。
        const auto matchingPages = collectMatchingPages(chunk, columnIndex);
        if (matchingPages.empty()) {
          candidateRows.clear();
          break;
        }

        // 再与当前候选行求交，减少后续 selective 解码输入。
        auto pageCandidates = collectRowsForMatchingPages(chunk, matchingPages, candidateRows);
        if (pageCandidates.empty()) {
          candidateRows.clear();
          break;
        }

        const auto kind = reader_->rowType_->childAt(columnIndex)->kind();
        // 仅对候选行做 selective 解码。
        auto decoded = decodeColumnSelective(chunk, kind, pageCandidates.data(),
                                             static_cast<vector_size_t>(pageCandidates.size()), &matchingPages);

        // 应用该列过滤条件，继续收缩候选行集合。
        std::vector<vector_size_t> nextCandidates;
        nextCandidates.reserve(pageCandidates.size());
        for (size_t i = 0; i < pageCandidates.size(); ++i) {
          bool pass = true;
          for (auto filterIndex : filterIndicesByColumn_[columnIndex]) {
            const auto &filter = filters_[filterIndex];
            if (!columnMatches(decoded, static_cast<vector_size_t>(i), filter.op, filter.value)) {
              pass = false;
              break;
            }
          }
          if (pass) {
            nextCandidates.push_back(pageCandidates[i]);
          }
        }

        candidateRows = std::move(nextCandidates);
        if (candidateRows.empty()) {
          break;
        }
      }
      cache.selectedRows = std::move(candidateRows);
    }

    if (cache.selectedRows.empty()) {
      continue;
    }
    currentGroup_ = std::move(cache);
    rowOffsetInGroup_ = 0;
    return true;
  }
  clearPageDataCache();
  return false;
}

bool FvxRowReader::rowGroupMatches(const FvxReader::RowGroup &rowGroup) const {
  // Row Group 级谓词下推：任一过滤列不可能命中则整组跳过。
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

std::vector<size_t> FvxRowReader::collectMatchingPages(const FvxReader::ColumnChunk &chunk, size_t columnIndex) const {
  // 用 page 级 min/max 先做第一层裁剪。
  std::vector<size_t> pages;
  pages.reserve(chunk.pages.size());
  for (size_t pageIndex = 0; pageIndex < chunk.pages.size(); ++pageIndex) {
    const auto &page = chunk.pages[pageIndex];
    bool mayMatch = true;
    for (auto filterIndex : filterIndicesByColumn_[columnIndex]) {
      const auto &filter = filters_[filterIndex];
      if (!columnMayMatch(page.stats, filter.op, filter.value)) {
        mayMatch = false;
        break;
      }
    }
    if (mayMatch) {
      pages.push_back(pageIndex);
    }
  }
  return pages;
}

std::vector<vector_size_t> FvxRowReader::collectRowsForMatchingPages(const FvxReader::ColumnChunk &chunk,
                                                                     const std::vector<size_t> &matchingPages,
                                                                     const std::vector<vector_size_t> &candidateRows) const {
  // 线性合并候选行与候选页范围，得到更小的解码输入。
  std::vector<vector_size_t> rows;
  if (matchingPages.empty() || candidateRows.empty()) {
    return rows;
  }

  rows.reserve(candidateRows.size());
  size_t pageCursor = 0;
  uint32_t pageStart = chunk.pages[matchingPages[pageCursor]].startRow;
  uint32_t pageEnd = pageStart + chunk.pages[matchingPages[pageCursor]].rowCount;

  for (auto row : candidateRows) {
    VELOX_CHECK(row >= 0, "FVX: negative row index");
    const auto rowValue = static_cast<uint32_t>(row);
    while (pageCursor < matchingPages.size() && rowValue >= pageEnd) {
      ++pageCursor;
      if (pageCursor < matchingPages.size()) {
        pageStart = chunk.pages[matchingPages[pageCursor]].startRow;
        pageEnd = pageStart + chunk.pages[matchingPages[pageCursor]].rowCount;
      }
    }
    if (pageCursor >= matchingPages.size()) {
      break;
    }
    if (rowValue >= pageStart) {
      rows.push_back(row);
    }
  }
  return rows;
}

std::string_view FvxRowReader::readPageDataCached(const FvxReader::ColumnChunk::Page &page) const {
  // 先查缓存，命中时避免重复 IO。
  auto it = pageDataCache_.find(page.offset);
  if (it != pageDataCache_.end()) {
    VELOX_CHECK_EQ(it->second.size(), page.length, "FVX: cached page length mismatch");
    return it->second;
  }

  if (pageDataCacheOrder_.size() >= kPageDataCacheCapacity) {
    // 容量满时淘汰最早写入的页（简单 LRU 近似）。
    const auto evictedOffset = pageDataCacheOrder_.front();
    pageDataCacheOrder_.pop_front();
    pageDataCache_.erase(evictedOffset);
  }

  auto pageData = reader_->file_->pread(page.offset, page.length);
  auto [insertedIt, _] = pageDataCache_.emplace(page.offset, std::move(pageData));
  pageDataCacheOrder_.push_back(page.offset);
  VELOX_CHECK_EQ(insertedIt->second.size(), page.length, "FVX: page length mismatch");
  return insertedIt->second;
}

void FvxRowReader::clearPageDataCache() const {
  // 在行组边界清理缓存，控制内存并避免跨组干扰。
  pageDataCache_.clear();
  pageDataCacheOrder_.clear();
}

RowVectorPtr FvxRowReader::buildRowVectorFromCache(const RowGroupCache &cache, const vector_size_t *rows,
                                                   vector_size_t count) const {
  VELOX_CHECK_NOT_NULL(cache.rowGroup, "FVX: row group cache is not initialized");
  std::vector<VectorPtr> children;
  children.reserve(projectedIndices_.size());

  // 延迟物化：仅解码当前 batch 的投影列与目标行。
  for (size_t i = 0; i < projectedIndices_.size(); ++i) {
    const auto columnIndex = projectedIndices_[i];
    const auto kind = reader_->rowType_->childAt(columnIndex)->kind();
    auto column = decodeColumnSelective(cache.rowGroup->columns[columnIndex], kind, rows, count, nullptr);
    auto type = outputType_->childAt(i);
    if (column.kind == TypeKind::BIGINT) {
      auto buffer = AlignedBuffer::allocate(count * sizeof(int64_t), reader_->pool_);
      auto *rawValues = buffer->asMutable<int64_t>();
      for (vector_size_t row = 0; row < count; ++row) {
        rawValues[row] = column.int64s[row];
      }
      children.push_back(std::make_shared<FlatVector<int64_t>>(reader_->pool_, type, nullptr, count, buffer));
    } else if (column.kind == TypeKind::INTEGER) {
      auto buffer = AlignedBuffer::allocate(count * sizeof(int32_t), reader_->pool_);
      auto *rawValues = buffer->asMutable<int32_t>();
      for (vector_size_t row = 0; row < count; ++row) {
        rawValues[row] = column.int32s[row];
      }
      children.push_back(std::make_shared<FlatVector<int32_t>>(reader_->pool_, type, nullptr, count, buffer));
    } else if (column.kind == TypeKind::VARCHAR) {
      size_t totalSize = 0;
      for (vector_size_t row = 0; row < count; ++row) {
        totalSize += column.strings[row].size();
      }
      auto values = AlignedBuffer::allocate(count * sizeof(StringView), reader_->pool_);
      auto *rawValues = values->asMutable<StringView>();
      auto dataBuffer = AlignedBuffer::allocate(totalSize, reader_->pool_);
      char *cursor = dataBuffer->asMutable<char>();
      size_t bytesOffset = 0;
      for (vector_size_t row = 0; row < count; ++row) {
        const auto &value = column.strings[row];
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

FvxRowReader::ColumnBuffer FvxRowReader::decodeColumnSelective(const FvxReader::ColumnChunk &chunk, TypeKind kind,
                                                               const vector_size_t *rows, vector_size_t count,
                                                               const std::vector<size_t> *pages) const {
  ColumnBuffer buffer;
  buffer.kind = kind;
  if (count == 0) {
    return buffer;
  }

  // selective 行号要求有序，便于按页线性推进。
  for (vector_size_t i = 0; i < count; ++i) {
    VELOX_CHECK(rows[i] >= 0, "FVX: negative row index");
    if (i > 0) {
      VELOX_CHECK(rows[i - 1] <= rows[i], "FVX: selective rows must be sorted");
    }
  }

  if (kind == TypeKind::BIGINT) {
    buffer.int64s.resize(count);
  } else if (kind == TypeKind::INTEGER) {
    buffer.int32s.resize(count);
  } else if (kind == TypeKind::VARCHAR) {
    buffer.strings.resize(count);
  } else {
    VELOX_FAIL("FVX supports BIGINT, INTEGER, VARCHAR only");
  }

  auto decodePage = [&](const FvxReader::ColumnChunk::Page &page, size_t &rowCursor) {
    if (rowCursor >= static_cast<size_t>(count)) {
      return;
    }

    const uint32_t pageStart = page.startRow;
    const uint32_t pageEnd = page.startRow + page.rowCount;
    while (rowCursor < static_cast<size_t>(count) && static_cast<uint32_t>(rows[rowCursor]) < pageStart) {
      VELOX_FAIL("FVX: selective rows contain gap before page start");
    }

    if (rowCursor >= static_cast<size_t>(count) || static_cast<uint32_t>(rows[rowCursor]) >= pageEnd) {
      return;
    }

    // 对齐到当前页内的行区间，只解码落入该页的目标行。
    const size_t pageRowStart = rowCursor;
    while (rowCursor < static_cast<size_t>(count) && static_cast<uint32_t>(rows[rowCursor]) < pageEnd) {
      ++rowCursor;
    }

    // 页数据统一走缓存读取，命中时无需再次 pread。
    const auto pageData = readPageDataCached(page);
    const auto parsedPage = parsePageHeader(pageData);
    VELOX_CHECK(parsedPage.header.pageType == 0, "FVX: unsupported page type");
    VELOX_CHECK(parsedPage.header.encoding == 0, "FVX: unsupported page encoding");
    VELOX_CHECK(parsedPage.header.rowCount == page.rowCount, "FVX: page row count mismatch");
    VELOX_CHECK(parsedPage.header.uncompressedSize == page.uncompressedSize, "FVX: page uncompressed size mismatch");
    VELOX_CHECK(parsedPage.header.compressedSize == page.compressedSize, "FVX: page compressed size mismatch");
    VELOX_CHECK(parsedPage.header.compressedSize == parsedPage.header.uncompressedSize,
                "FVX: compressed pages are unsupported");
    std::string_view payload(pageData.data() + parsedPage.payloadOffset, pageData.size() - parsedPage.payloadOffset);

    if (kind == TypeKind::BIGINT) {
      VELOX_CHECK(payload.size() == static_cast<size_t>(page.rowCount) * sizeof(int64_t),
                  "FVX: bigint page payload size mismatch");
      for (size_t i = pageRowStart; i < rowCursor; ++i) {
        const auto rowInPage = static_cast<uint32_t>(rows[i]) - pageStart;
        int64_t value;
        std::memcpy(&value, payload.data() + static_cast<size_t>(rowInPage) * sizeof(int64_t), sizeof(int64_t));
        buffer.int64s[i] = value;
      }
    } else if (kind == TypeKind::INTEGER) {
      VELOX_CHECK(payload.size() == static_cast<size_t>(page.rowCount) * sizeof(int32_t),
                  "FVX: integer page payload size mismatch");
      for (size_t i = pageRowStart; i < rowCursor; ++i) {
        const auto rowInPage = static_cast<uint32_t>(rows[i]) - pageStart;
        int32_t value;
        std::memcpy(&value, payload.data() + static_cast<size_t>(rowInPage) * sizeof(int32_t), sizeof(int32_t));
        buffer.int32s[i] = value;
      }
    } else {
      size_t offset = 0;
      uint32_t totalBytes = readPod<uint32_t>(payload, offset);
      std::vector<uint32_t> offsets(page.rowCount + 1);
      const auto offsetsByteSize = offsets.size() * sizeof(uint32_t);
      VELOX_CHECK(offset + offsetsByteSize + totalBytes == payload.size(), "FVX: varchar page payload size mismatch");
      std::memcpy(offsets.data(), payload.data() + offset, offsetsByteSize);
      offset += offsetsByteSize;
      const char *base = payload.data() + offset;
      for (size_t i = pageRowStart; i < rowCursor; ++i) {
        const auto rowInPage = static_cast<uint32_t>(rows[i]) - pageStart;
        const uint32_t start = offsets[rowInPage];
        const uint32_t end = offsets[rowInPage + 1];
        VELOX_CHECK(end >= start, "FVX: invalid string offsets");
        buffer.strings[i] = std::string(base + start, end - start);
      }
      VELOX_CHECK(offsets.back() == totalBytes, "FVX: string data length mismatch");
    }
  };

  size_t rowCursor = 0;
  if (pages != nullptr) {
    // 过滤路径：仅遍历统计命中的页。
    for (auto pageIndex : *pages) {
      decodePage(chunk.pages[pageIndex], rowCursor);
      if (rowCursor >= static_cast<size_t>(count)) {
        break;
      }
    }
  } else {
    // 投影路径：按页顺序扫描并挑出当前 batch 需要的行。
    for (const auto &page : chunk.pages) {
      decodePage(page, rowCursor);
      if (rowCursor >= static_cast<size_t>(count)) {
        break;
      }
    }
  }

  VELOX_CHECK(rowCursor == static_cast<size_t>(count), "FVX: selective rows out of page bounds");
  return buffer;
}

void FvxRowReader::buildProjection() {
  projectedIndices_.clear();

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
  filterIndicesByColumn_.clear();
  filterIndicesByColumn_.resize(reader_->rowType_->size());
  filters_.reserve(options_.filters().size());
  for (size_t i = 0; i < options_.filters().size(); ++i) {
    const auto &filter = options_.filters()[i];
    auto it = reader_->nameToIndex_.find(filter.column);
    VELOX_CHECK(it != reader_->nameToIndex_.end(), "Unknown filter column {}", filter.column);
    filters_.push_back(PreparedFilter{it->second, filter.op, filter.value});
    filterColumnIndices_.push_back(it->second);
    filterIndicesByColumn_[it->second].push_back(i);
  }

  std::sort(filterColumnIndices_.begin(), filterColumnIndices_.end());
  filterColumnIndices_.erase(std::unique(filterColumnIndices_.begin(), filterColumnIndices_.end()),
                             filterColumnIndices_.end());
}

} // namespace facebook::velox::dwio::fvx
