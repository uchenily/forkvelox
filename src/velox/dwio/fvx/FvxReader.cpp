#include "velox/dwio/fvx/FvxReader.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
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
      std::vector<uint8_t> candidateRows(rowGroup.rowCount, 1);
      std::vector<uint8_t> columnCandidateRows(rowGroup.rowCount, 0);
      bool hasAnyCandidate = true;

      // Decode only candidate pages for filter columns first.
      for (auto columnIndex : filterColumnIndices_) {
        const auto &chunk = rowGroup.columns[columnIndex];
        const auto matchingPages = collectMatchingPages(chunk, columnIndex);
        if (matchingPages.empty()) {
          hasAnyCandidate = false;
          break;
        }

        std::fill(columnCandidateRows.begin(), columnCandidateRows.end(), 0);
        for (auto pageIndex : matchingPages) {
          const auto &page = chunk.pages[pageIndex];
          for (uint32_t row = page.startRow; row < page.startRow + page.rowCount; ++row) {
            columnCandidateRows[row] = 1;
          }
        }

        hasAnyCandidate = false;
        for (uint32_t row = 0; row < rowGroup.rowCount; ++row) {
          candidateRows[row] = static_cast<uint8_t>(candidateRows[row] && columnCandidateRows[row]);
          hasAnyCandidate = hasAnyCandidate || (candidateRows[row] != 0);
        }
        if (!hasAnyCandidate) {
          break;
        }

        auto kind = reader_->rowType_->childAt(columnIndex)->kind();
        cache.columns[columnIndex] = decodeColumn(chunk, kind, rowGroup.rowCount, matchingPages);
      }

      if (!hasAnyCandidate) {
        continue;
      }

      for (vector_size_t row = 0; row < static_cast<vector_size_t>(rowGroup.rowCount); ++row) {
        if (!candidateRows[row]) {
          continue;
        }
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
      const auto matchingPages = collectPagesForRows(rowGroup.columns[columnIndex], cache.selectedRows);
      if (matchingPages.empty()) {
        continue;
      }
      auto kind = reader_->rowType_->childAt(columnIndex)->kind();
      cache.columns[columnIndex] = decodeColumn(rowGroup.columns[columnIndex], kind, rowGroup.rowCount, matchingPages);
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

std::vector<size_t> FvxRowReader::collectMatchingPages(const FvxReader::ColumnChunk &chunk, size_t columnIndex) const {
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

std::vector<size_t> FvxRowReader::collectPagesForRows(const FvxReader::ColumnChunk &chunk,
                                                      const std::vector<vector_size_t> &rows) const {
  std::vector<size_t> pages;
  if (rows.empty()) {
    return pages;
  }

  size_t rowIndex = 0;
  for (size_t pageIndex = 0; pageIndex < chunk.pages.size(); ++pageIndex) {
    const auto &page = chunk.pages[pageIndex];
    const uint32_t pageStart = page.startRow;
    const uint32_t pageEnd = page.startRow + page.rowCount;
    while (rowIndex < rows.size() && static_cast<uint32_t>(rows[rowIndex]) < pageStart) {
      ++rowIndex;
    }
    if (rowIndex >= rows.size()) {
      break;
    }
    if (static_cast<uint32_t>(rows[rowIndex]) < pageEnd) {
      pages.push_back(pageIndex);
      while (rowIndex < rows.size() && static_cast<uint32_t>(rows[rowIndex]) < pageEnd) {
        ++rowIndex;
      }
    }
  }
  return pages;
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
                                                      uint32_t rowCount, const std::vector<size_t> &pages) const {
  ColumnBuffer buffer;
  buffer.kind = kind;
  if (pages.empty()) {
    return buffer;
  }

  if (kind == TypeKind::BIGINT) {
    buffer.int64s.resize(rowCount);
    for (auto pageIndex : pages) {
      const auto &page = chunk.pages[pageIndex];
      auto pageData = reader_->file_->pread(page.offset, page.length);
      const auto parsedPage = parsePageHeader(pageData);
      VELOX_CHECK(parsedPage.header.pageType == 0, "FVX: unsupported page type");
      VELOX_CHECK(parsedPage.header.encoding == 0, "FVX: unsupported page encoding");
      VELOX_CHECK(parsedPage.header.rowCount == page.rowCount, "FVX: page row count mismatch");
      VELOX_CHECK(parsedPage.header.uncompressedSize == page.uncompressedSize, "FVX: page uncompressed size mismatch");
      VELOX_CHECK(parsedPage.header.compressedSize == page.compressedSize, "FVX: page compressed size mismatch");
      VELOX_CHECK(parsedPage.header.compressedSize == parsedPage.header.uncompressedSize,
                  "FVX: compressed pages are unsupported");
      const auto byteSize = static_cast<size_t>(page.rowCount) * sizeof(int64_t);
      const auto payloadSize = pageData.size() - parsedPage.payloadOffset;
      VELOX_CHECK(byteSize == payloadSize, "FVX: bigint page payload size mismatch");
      std::memcpy(buffer.int64s.data() + page.startRow, pageData.data() + parsedPage.payloadOffset, byteSize);
    }
  } else if (kind == TypeKind::INTEGER) {
    buffer.int32s.resize(rowCount);
    for (auto pageIndex : pages) {
      const auto &page = chunk.pages[pageIndex];
      auto pageData = reader_->file_->pread(page.offset, page.length);
      const auto parsedPage = parsePageHeader(pageData);
      VELOX_CHECK(parsedPage.header.pageType == 0, "FVX: unsupported page type");
      VELOX_CHECK(parsedPage.header.encoding == 0, "FVX: unsupported page encoding");
      VELOX_CHECK(parsedPage.header.rowCount == page.rowCount, "FVX: page row count mismatch");
      VELOX_CHECK(parsedPage.header.uncompressedSize == page.uncompressedSize, "FVX: page uncompressed size mismatch");
      VELOX_CHECK(parsedPage.header.compressedSize == page.compressedSize, "FVX: page compressed size mismatch");
      VELOX_CHECK(parsedPage.header.compressedSize == parsedPage.header.uncompressedSize,
                  "FVX: compressed pages are unsupported");
      const auto byteSize = static_cast<size_t>(page.rowCount) * sizeof(int32_t);
      const auto payloadSize = pageData.size() - parsedPage.payloadOffset;
      VELOX_CHECK(byteSize == payloadSize, "FVX: integer page payload size mismatch");
      std::memcpy(buffer.int32s.data() + page.startRow, pageData.data() + parsedPage.payloadOffset, byteSize);
    }
  } else if (kind == TypeKind::VARCHAR) {
    buffer.strings.resize(rowCount);
    for (auto pageIndex : pages) {
      const auto &page = chunk.pages[pageIndex];
      auto pageData = reader_->file_->pread(page.offset, page.length);
      const auto parsedPage = parsePageHeader(pageData);
      VELOX_CHECK(parsedPage.header.pageType == 0, "FVX: unsupported page type");
      VELOX_CHECK(parsedPage.header.encoding == 0, "FVX: unsupported page encoding");
      VELOX_CHECK(parsedPage.header.rowCount == page.rowCount, "FVX: page row count mismatch");
      VELOX_CHECK(parsedPage.header.uncompressedSize == page.uncompressedSize, "FVX: page uncompressed size mismatch");
      VELOX_CHECK(parsedPage.header.compressedSize == page.compressedSize, "FVX: page compressed size mismatch");
      VELOX_CHECK(parsedPage.header.compressedSize == parsedPage.header.uncompressedSize,
                  "FVX: compressed pages are unsupported");

      std::string_view payload(pageData.data() + parsedPage.payloadOffset, pageData.size() - parsedPage.payloadOffset);
      size_t offset = 0;
      uint32_t totalBytes = readPod<uint32_t>(payload, offset);
      std::vector<uint32_t> offsets(page.rowCount + 1);
      const auto offsetsByteSize = offsets.size() * sizeof(uint32_t);
      VELOX_CHECK(offset + offsetsByteSize + totalBytes == payload.size(), "FVX: varchar page payload size mismatch");
      std::memcpy(offsets.data(), payload.data() + offset, offsetsByteSize);
      offset += offsets.size() * sizeof(uint32_t);
      const char *base = payload.data() + offset;
      for (uint32_t i = 0; i < page.rowCount; ++i) {
        uint32_t start = offsets[i];
        uint32_t end = offsets[i + 1];
        VELOX_CHECK(end >= start, "FVX: invalid string offsets");
        buffer.strings[page.startRow + i] = std::string(base + start, end - start);
      }
      VELOX_CHECK(offsets.back() == totalBytes, "FVX: string data length mismatch");
    }
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
