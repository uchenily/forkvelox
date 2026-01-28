#include "velox/dwio/fvx/FvxWriter.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <mutex>
#include <utility>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::dwio::fvx {
namespace {

constexpr char kMagic[] = {'F', 'V', 'X', '1'};
constexpr uint32_t kVersion = 1;

template <typename T>
void appendPod(std::string& out, T value) {
  out.append(reinterpret_cast<const char*>(&value), sizeof(T));
}

void appendString(std::string& out, const std::string& value) {
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
  std::string data;
  uint64_t offset{0};
  uint64_t length{0};
};

struct RowGroupData {
  uint32_t rowCount{0};
  std::vector<ColumnChunkData> columns;
};

void ensureFileSystemRegistered() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    filesystems::registerLocalFileSystem();
  });
}

ColumnStats buildStats(
    TypeKind kind,
    const RowVector& data,
    size_t columnIndex,
    vector_size_t start,
    vector_size_t count) {
  ColumnStats stats;
  stats.kind = kind;
  if (kind == TypeKind::BIGINT) {
    auto vector = std::dynamic_pointer_cast<FlatVector<int64_t>>(
        data.childAt(columnIndex));
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
    auto vector = std::dynamic_pointer_cast<FlatVector<int32_t>>(
        data.childAt(columnIndex));
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
    auto vector = std::dynamic_pointer_cast<FlatVector<StringView>>(
        data.childAt(columnIndex));
    VELOX_CHECK(vector != nullptr, "FVX supports FlatVector<StringView> only");
    for (vector_size_t i = 0; i < count; ++i) {
      VELOX_CHECK(!vector->isNullAt(start + i), "FVX does not support nulls");
    }
    std::string minValue(
        vector->valueAt(start).data(),
        vector->valueAt(start).size());
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

std::string buildColumnData(
    TypeKind kind,
    const RowVector& data,
    size_t columnIndex,
    vector_size_t start,
    vector_size_t count) {
  std::string out;
  if (kind == TypeKind::BIGINT) {
    auto vector = std::dynamic_pointer_cast<FlatVector<int64_t>>(
        data.childAt(columnIndex));
    VELOX_CHECK(vector != nullptr, "FVX supports FlatVector<int64_t> only");
    out.resize(count * sizeof(int64_t));
    std::memcpy(
        out.data(),
        vector->rawValues() + start,
        count * sizeof(int64_t));
  } else if (kind == TypeKind::INTEGER) {
    auto vector = std::dynamic_pointer_cast<FlatVector<int32_t>>(
        data.childAt(columnIndex));
    VELOX_CHECK(vector != nullptr, "FVX supports FlatVector<int32_t> only");
    out.resize(count * sizeof(int32_t));
    std::memcpy(
        out.data(),
        vector->rawValues() + start,
        count * sizeof(int32_t));
  } else if (kind == TypeKind::VARCHAR) {
    auto vector = std::dynamic_pointer_cast<FlatVector<StringView>>(
        data.childAt(columnIndex));
    VELOX_CHECK(vector != nullptr, "FVX supports FlatVector<StringView> only");
    std::vector<uint32_t> offsets(count + 1, 0);
    size_t totalBytes = 0;
    for (vector_size_t i = 0; i < count; ++i) {
      auto view = vector->valueAt(start + i);
      totalBytes += view.size();
      offsets[i + 1] = static_cast<uint32_t>(totalBytes);
    }
    appendPod(out, static_cast<uint32_t>(totalBytes));
    out.append(reinterpret_cast<const char*>(offsets.data()),
               offsets.size() * sizeof(uint32_t));
    size_t bytesOffset = out.size();
    out.resize(out.size() + totalBytes);
    char* cursor = out.data() + bytesOffset;
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

size_t statsSize(const ColumnStats& stats) {
  if (stats.kind == TypeKind::BIGINT) {
    return sizeof(int64_t) * 2;
  }
  if (stats.kind == TypeKind::INTEGER) {
    return sizeof(int32_t) * 2;
  }
  if (stats.kind == TypeKind::VARCHAR) {
    return sizeof(uint32_t) + stats.minString.size() + sizeof(uint32_t) +
        stats.maxString.size();
  }
  return 0;
}

} // namespace

void FvxWriter::write(
    const RowVector& data,
    const std::string& path,
    FvxWriteOptions options) {
  ensureFileSystemRegistered();
  auto fs = filesystems::getFileSystem(path, nullptr);

  filesystems::FileOptions fileOptions;
  fileOptions.shouldThrowOnFileAlreadyExists = false;
  fileOptions.shouldCreateParentDirectories = true;
  auto file = fs->openFileForWrite(path, fileOptions);

  auto rowType = asRowType(data.type());
  VELOX_CHECK(rowType != nullptr, "FVX requires ROW type input");
  VELOX_CHECK(
      rowType->size() > 0,
      "FVX requires at least one column");

  const vector_size_t totalRows = data.size();
  const vector_size_t groupSize =
      options.rowGroupSize == 0 ? totalRows : options.rowGroupSize;

  std::vector<RowGroupData> rowGroups;
  for (vector_size_t start = 0; start < totalRows; start += groupSize) {
    vector_size_t count =
        std::min<vector_size_t>(groupSize, totalRows - start);
    RowGroupData group;
    group.rowCount = static_cast<uint32_t>(count);
    group.columns.reserve(rowType->size());
    for (size_t col = 0; col < rowType->size(); ++col) {
      auto kind = rowType->childAt(col)->kind();
      ColumnChunkData chunk;
      chunk.stats = buildStats(kind, data, col, start, count);
      chunk.data = buildColumnData(kind, data, col, start, count);
      chunk.length = static_cast<uint64_t>(chunk.data.size());
      group.columns.push_back(std::move(chunk));
    }
    rowGroups.push_back(std::move(group));
  }

  size_t schemaSize = sizeof(uint32_t);
  for (size_t col = 0; col < rowType->size(); ++col) {
    schemaSize += sizeof(uint32_t);
    schemaSize += rowType->nameOf(col).size();
    schemaSize += sizeof(uint8_t);
  }

  size_t metadataSize = sizeof(uint32_t);
  for (const auto& group : rowGroups) {
    metadataSize += sizeof(uint32_t);
    for (const auto& column : group.columns) {
      metadataSize += sizeof(uint64_t) * 2;
      metadataSize += statsSize(column.stats);
    }
  }

  size_t headerSize = sizeof(kMagic) + sizeof(uint32_t) + schemaSize;
  uint64_t dataStart = headerSize + metadataSize;
  uint64_t cursor = dataStart;
  for (auto& group : rowGroups) {
    for (auto& column : group.columns) {
      column.offset = cursor;
      cursor += column.length;
    }
  }

  std::string out;
  out.reserve(static_cast<size_t>(cursor));
  out.append(kMagic, sizeof(kMagic));
  appendPod(out, kVersion);
  appendPod(out, static_cast<uint32_t>(rowType->size()));
  for (size_t col = 0; col < rowType->size(); ++col) {
    appendString(out, rowType->nameOf(col));
    auto kind = rowType->childAt(col)->kind();
    appendPod(out, static_cast<uint8_t>(kind));
  }

  appendPod(out, static_cast<uint32_t>(rowGroups.size()));
  for (const auto& group : rowGroups) {
    appendPod(out, group.rowCount);
    for (const auto& column : group.columns) {
      appendPod(out, column.offset);
      appendPod(out, column.length);
      const auto& stats = column.stats;
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
  }

  for (const auto& group : rowGroups) {
    for (const auto& column : group.columns) {
      out.append(column.data);
    }
  }

  file->truncate(0);
  file->append(out);
  file->close();
}

} // namespace facebook::velox::dwio::fvx
