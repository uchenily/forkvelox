#include "velox/dwio/fvx/FvxWriter.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::dwio::fvx {
namespace {

constexpr char kMagic[] = {'F', 'V', 'X', '3'};
constexpr uint32_t kVersion = 3;

template <typename T>
void appendPod(std::string &out, T value) {
  out.append(reinterpret_cast<const char *>(&value), sizeof(T));
}

void appendString(std::string &out, const std::string &value) {
  uint32_t length = static_cast<uint32_t>(value.size());
  appendPod(out, length);
  out.append(value);
}

struct ColumnStats {
  TypeKind kind{TypeKind::UNKNOWN};
  int64_t minBigint{0};
  int64_t maxBigint{0};
  int32_t minInt{0};
  int32_t maxInt{0};
  std::string minString;
  std::string maxString;
};

struct ColumnChunkData {
  ColumnStats stats;
  struct PageData {
    uint32_t startRow{0};
    uint32_t rowCount{0};
    ColumnStats stats;
    std::string data;
    uint64_t offset{0};
    uint64_t length{0};
    uint32_t uncompressedSize{0};
    uint32_t compressedSize{0};
  };

  std::vector<PageData> pages;
  uint64_t offset{0};
  uint64_t length{0};
};

struct RowGroupData {
  uint32_t rowCount{0};
  std::vector<ColumnChunkData> columns;
};

void ensureFileSystemRegistered() {
  static std::once_flag flag;
  std::call_once(flag, []() { filesystems::registerLocalFileSystem(); });
}

ColumnStats buildStats(TypeKind kind, const RowVector &data, size_t columnIndex, vector_size_t start,
                       vector_size_t count) {
  ColumnStats stats;
  stats.kind = kind;
  if (kind == TypeKind::BIGINT) {
    auto vector = std::dynamic_pointer_cast<FlatVector<int64_t>>(data.childAt(columnIndex));
    VELOX_CHECK(vector != nullptr, "FVX supports FlatVector<int64_t> only");
    for (vector_size_t i = 0; i < count; ++i) {
      VELOX_CHECK(!vector->isNullAt(start + i), "FVX does not support nulls");
    }
    int64_t minValue = vector->valueAt(start);
    int64_t maxValue = minValue;
    for (vector_size_t i = 1; i < count; ++i) {
      int64_t value = vector->valueAt(start + i);
      minValue = std::min(minValue, value);
      maxValue = std::max(maxValue, value);
    }
    stats.minBigint = minValue;
    stats.maxBigint = maxValue;
  } else if (kind == TypeKind::INTEGER) {
    auto vector = std::dynamic_pointer_cast<FlatVector<int32_t>>(data.childAt(columnIndex));
    VELOX_CHECK(vector != nullptr, "FVX supports FlatVector<int32_t> only");
    for (vector_size_t i = 0; i < count; ++i) {
      VELOX_CHECK(!vector->isNullAt(start + i), "FVX does not support nulls");
    }
    int32_t minValue = vector->valueAt(start);
    int32_t maxValue = minValue;
    for (vector_size_t i = 1; i < count; ++i) {
      int32_t value = vector->valueAt(start + i);
      minValue = std::min(minValue, value);
      maxValue = std::max(maxValue, value);
    }
    stats.minInt = minValue;
    stats.maxInt = maxValue;
  } else if (kind == TypeKind::VARCHAR) {
    auto vector = std::dynamic_pointer_cast<FlatVector<StringView>>(data.childAt(columnIndex));
    VELOX_CHECK(vector != nullptr, "FVX supports FlatVector<StringView> only");
    for (vector_size_t i = 0; i < count; ++i) {
      VELOX_CHECK(!vector->isNullAt(start + i), "FVX does not support nulls");
    }
    std::string minValue(vector->valueAt(start).data(), vector->valueAt(start).size());
    std::string maxValue = minValue;
    for (vector_size_t i = 1; i < count; ++i) {
      auto view = vector->valueAt(start + i);
      std::string value(view.data(), view.size());
      if (value < minValue) {
        minValue = value;
      }
      if (value > maxValue) {
        maxValue = value;
      }
    }
    stats.minString = std::move(minValue);
    stats.maxString = std::move(maxValue);
  } else {
    VELOX_FAIL("FVX supports BIGINT, INTEGER, VARCHAR only");
  }
  return stats;
}

std::string buildColumnData(TypeKind kind, const RowVector &data, size_t columnIndex, vector_size_t start,
                            vector_size_t count) {
  std::string out;
  if (kind == TypeKind::BIGINT) {
    auto vector = std::dynamic_pointer_cast<FlatVector<int64_t>>(data.childAt(columnIndex));
    VELOX_CHECK(vector != nullptr, "FVX supports FlatVector<int64_t> only");
    out.resize(count * sizeof(int64_t));
    std::memcpy(out.data(), vector->rawValues() + start, count * sizeof(int64_t));
  } else if (kind == TypeKind::INTEGER) {
    auto vector = std::dynamic_pointer_cast<FlatVector<int32_t>>(data.childAt(columnIndex));
    VELOX_CHECK(vector != nullptr, "FVX supports FlatVector<int32_t> only");
    out.resize(count * sizeof(int32_t));
    std::memcpy(out.data(), vector->rawValues() + start, count * sizeof(int32_t));
  } else if (kind == TypeKind::VARCHAR) {
    auto vector = std::dynamic_pointer_cast<FlatVector<StringView>>(data.childAt(columnIndex));
    VELOX_CHECK(vector != nullptr, "FVX supports FlatVector<StringView> only");
    std::vector<uint32_t> offsets(count + 1, 0);
    size_t totalBytes = 0;
    for (vector_size_t i = 0; i < count; ++i) {
      auto view = vector->valueAt(start + i);
      totalBytes += view.size();
      offsets[i + 1] = static_cast<uint32_t>(totalBytes);
    }
    appendPod(out, static_cast<uint32_t>(totalBytes));
    out.append(reinterpret_cast<const char *>(offsets.data()), offsets.size() * sizeof(uint32_t));
    size_t bytesOffset = out.size();
    out.resize(out.size() + totalBytes);
    char *cursor = out.data() + bytesOffset;
    for (vector_size_t i = 0; i < count; ++i) {
      auto view = vector->valueAt(start + i);
      std::memcpy(cursor, view.data(), view.size());
      cursor += view.size();
    }
  } else {
    VELOX_FAIL("FVX supports BIGINT, INTEGER, VARCHAR only");
  }
  return out;
}

void appendStats(std::string &out, const ColumnStats &stats) {
  if (stats.kind == TypeKind::BIGINT) {
    appendPod(out, stats.minBigint);
    appendPod(out, stats.maxBigint);
  } else if (stats.kind == TypeKind::INTEGER) {
    appendPod(out, stats.minInt);
    appendPod(out, stats.maxInt);
  } else if (stats.kind == TypeKind::VARCHAR) {
    appendString(out, stats.minString);
    appendString(out, stats.maxString);
  } else {
    VELOX_FAIL("FVX supports BIGINT, INTEGER, VARCHAR only");
  }
}

std::string buildPageHeader(uint32_t rowCount, uint32_t uncompressedSize, uint32_t compressedSize) {
  std::string header;
  // Parquet-like page header core fields.
  appendPod(header, static_cast<uint8_t>(0)); // DATA_PAGE
  appendPod(header, static_cast<uint8_t>(0)); // PLAIN encoding
  appendPod(header, rowCount);
  appendPod(header, uncompressedSize);
  appendPod(header, compressedSize);
  return header;
}

} // namespace

void FvxWriter::write(const RowVector &data, const std::string &path, FvxWriteOptions options) {
  ensureFileSystemRegistered();
  auto fs = filesystems::getFileSystem(path, nullptr);

  filesystems::FileOptions fileOptions;
  fileOptions.shouldThrowOnFileAlreadyExists = false;
  fileOptions.shouldCreateParentDirectories = true;
  auto file = fs->openFileForWrite(path, fileOptions);

  auto rowType = asRowType(data.type());
  VELOX_CHECK(rowType != nullptr, "FVX requires ROW type input");
  VELOX_CHECK(rowType->size() > 0, "FVX requires at least one column");

  const vector_size_t totalRows = data.size();
  const vector_size_t groupSize = options.rowGroupSize == 0 ? totalRows : options.rowGroupSize;
  const vector_size_t pageSize = options.pageSize == 0 ? groupSize : static_cast<vector_size_t>(options.pageSize);
  VELOX_CHECK(pageSize > 0, "FVX pageSize must be > 0");

  std::vector<RowGroupData> rowGroups;
  for (vector_size_t start = 0; start < totalRows; start += groupSize) {
    vector_size_t count = std::min<vector_size_t>(groupSize, totalRows - start);
    RowGroupData group;
    group.rowCount = static_cast<uint32_t>(count);
    group.columns.reserve(rowType->size());
    for (size_t col = 0; col < rowType->size(); ++col) {
      auto kind = rowType->childAt(col)->kind();
      ColumnChunkData chunk;
      chunk.stats = buildStats(kind, data, col, start, count);
      for (vector_size_t pageStart = 0; pageStart < count; pageStart += pageSize) {
        const vector_size_t pageRowCount = std::min<vector_size_t>(pageSize, count - pageStart);
        ColumnChunkData::PageData page;
        page.startRow = static_cast<uint32_t>(pageStart);
        page.rowCount = static_cast<uint32_t>(pageRowCount);
        page.stats = buildStats(kind, data, col, start + pageStart, pageRowCount);
        page.data = buildColumnData(kind, data, col, start + pageStart, pageRowCount);
        page.uncompressedSize = static_cast<uint32_t>(page.data.size());
        page.compressedSize = page.uncompressedSize;
        chunk.pages.push_back(std::move(page));
      }
      VELOX_CHECK(!chunk.pages.empty(), "FVX requires at least one page per chunk");
      group.columns.push_back(std::move(chunk));
    }
    rowGroups.push_back(std::move(group));
  }

  std::string out;
  out.append(kMagic, sizeof(kMagic)); // Prefix magic, parquet-like.

  for (auto &group : rowGroups) {
    for (auto &column : group.columns) {
      VELOX_CHECK(!column.pages.empty(), "FVX requires at least one page per chunk");
      column.offset = static_cast<uint64_t>(out.size());
      for (auto &page : column.pages) {
        page.offset = static_cast<uint64_t>(out.size());
        auto pageHeader = buildPageHeader(page.rowCount, page.uncompressedSize, page.compressedSize);
        appendPod(out, static_cast<uint32_t>(pageHeader.size()));
        out.append(pageHeader);
        out.append(page.data);
        page.length = static_cast<uint64_t>(out.size()) - page.offset;
      }
      column.length = static_cast<uint64_t>(out.size()) - column.offset;
    }
  }

  std::string footer;
  appendPod(footer, kVersion);
  appendPod(footer, static_cast<uint32_t>(rowType->size()));
  for (size_t col = 0; col < rowType->size(); ++col) {
    appendString(footer, rowType->nameOf(col));
    appendPod(footer, static_cast<uint8_t>(rowType->childAt(col)->kind()));
  }

  appendPod(footer, static_cast<uint32_t>(rowGroups.size()));
  for (const auto &group : rowGroups) {
    appendPod(footer, group.rowCount);
    for (const auto &column : group.columns) {
      appendPod(footer, column.offset);
      appendPod(footer, column.length);
      appendStats(footer, column.stats);
      appendPod(footer, static_cast<uint32_t>(column.pages.size()));
      for (const auto &page : column.pages) {
        appendPod(footer, page.startRow);
        appendPod(footer, page.rowCount);
        appendPod(footer, page.offset);
        appendPod(footer, page.length);
        appendPod(footer, page.uncompressedSize);
        appendPod(footer, page.compressedSize);
        appendStats(footer, page.stats);
      }
    }
  }

  VELOX_CHECK(footer.size() <= std::numeric_limits<uint32_t>::max(), "FVX footer is too large");
  out.append(footer);
  appendPod(out, static_cast<uint32_t>(footer.size()));
  out.append(kMagic, sizeof(kMagic)); // Suffix magic, parquet-like.

  file->truncate(0);
  file->append(out);
  file->close();
}

} // namespace facebook::velox::dwio::fvx
