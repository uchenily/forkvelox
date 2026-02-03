#include "velox/dwio/common/RowVectorFile.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::dwio::common {
namespace {

void ensureFileSystemRegistered() {
  static std::once_flag flag;
  std::call_once(flag, []() { filesystems::registerLocalFileSystem(); });
}

std::string trim(std::string value) {
  auto notSpace = [](unsigned char c) { return !std::isspace(c); };
  value.erase(value.begin(),
              std::find_if(value.begin(), value.end(), notSpace));
  value.erase(std::find_if(value.rbegin(), value.rend(), notSpace).base(),
              value.end());
  return value;
}

std::vector<std::string> split(const std::string &value, char delimiter) {
  std::vector<std::string> out;
  std::stringstream ss(value);
  std::string item;
  while (std::getline(ss, item, delimiter)) {
    out.push_back(item);
  }
  return out;
}

TypePtr parseType(const std::string &name) {
  if (name == "BIGINT") {
    return BIGINT();
  }
  if (name == "INTEGER") {
    return INTEGER();
  }
  if (name == "VARCHAR") {
    return VARCHAR();
  }
  VELOX_FAIL("Unsupported type: {}", name);
}

void writeInternal(const RowVector &data, const std::string &path,
                   bool includeHeader, bool append) {
  ensureFileSystemRegistered();
  auto fs = filesystems::getFileSystem(path, nullptr);

  filesystems::FileOptions options;
  options.shouldThrowOnFileAlreadyExists = false;
  options.shouldCreateParentDirectories = true;

  auto file = fs->openFileForWrite(path, options);

  if (!append) {
    file->truncate(0);
  }

  std::stringstream out;

  auto rowType = asRowType(data.type());
  VELOX_CHECK(rowType != nullptr, "RowVector must have ROW type");

  if (includeHeader) {
    out << "# schema: ";
    for (size_t i = 0; i < rowType->size(); ++i) {
      if (i > 0) {
        out << ",";
      }
      out << rowType->nameOf(i) << ":" << rowType->childAt(i)->toString();
    }
    out << "\n";
  }

  for (vector_size_t row = 0; row < data.size(); ++row) {
    for (size_t col = 0; col < rowType->size(); ++col) {
      if (col > 0) {
        out << "\t";
      }
      out << data.childAt(col)->toString(row);
    }
    out << "\n";
  }

  file->append(out.str());
  file->close();
}

} // namespace

void RowVectorFile::write(const RowVector &data, const std::string &path) {
  writeInternal(data, path, true, false);
}

void RowVectorFile::append(const RowVector &data, const std::string &path,
                           bool includeHeader) {
  writeInternal(data, path, includeHeader, !includeHeader);
}

RowVectorPtr RowVectorFile::read(memory::MemoryPool *pool,
                                 const std::string &path) {
  ensureFileSystemRegistered();
  auto fs = filesystems::getFileSystem(path, nullptr);
  auto file = fs->openFileForRead(path);

  // Read entire file content
  uint64_t fileSize = file->size();
  std::string content = file->pread(0, fileSize);
  std::stringstream in(content);

  std::string line;
  VELOX_CHECK(std::getline(in, line), "Empty file: {}", path);
  VELOX_CHECK(line.rfind("# schema:", 0) == 0, "Missing schema header in {}",
              path);

  std::string schema = trim(line.substr(std::string("# schema:").size()));
  auto fields = split(schema, ',');
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(fields.size());
  types.reserve(fields.size());

  for (const auto &field : fields) {
    auto trimmed = trim(field);
    auto parts = split(trimmed, ':');
    VELOX_CHECK(parts.size() == 2, "Invalid schema field: {}", trimmed);
    names.push_back(trim(parts[0]));
    types.push_back(parseType(trim(parts[1])));
  }

  struct ColumnData {
    TypeKind kind;
    std::vector<int64_t> ints;
    std::vector<int32_t> int32s;
    std::vector<std::string> strings;
  };

  std::vector<ColumnData> columns;
  columns.reserve(types.size());
  for (const auto &type : types) {
    ColumnData column;
    column.kind = type->kind();
    columns.push_back(std::move(column));
  }

  while (std::getline(in, line)) {
    if (line.empty()) {
      continue;
    }
    auto values = split(line, '\t');
    VELOX_CHECK(values.size() == columns.size(), "Row column count mismatch");

    for (size_t i = 0; i < columns.size(); ++i) {
      auto value = values[i];
      auto &column = columns[i];
      if (column.kind == TypeKind::BIGINT) {
        column.ints.push_back(std::stoll(value));
      } else if (column.kind == TypeKind::INTEGER) {
        column.int32s.push_back(static_cast<int32_t>(std::stol(value)));
      } else if (column.kind == TypeKind::VARCHAR) {
        column.strings.push_back(value);
      } else {
        VELOX_FAIL("Unsupported type in data row");
      }
    }
  }

  vector_size_t rowCount = 0;
  if (!columns.empty()) {
    auto &column = columns[0];
    if (column.kind == TypeKind::BIGINT) {
      rowCount = static_cast<vector_size_t>(column.ints.size());
    } else if (column.kind == TypeKind::INTEGER) {
      rowCount = static_cast<vector_size_t>(column.int32s.size());
    } else {
      rowCount = static_cast<vector_size_t>(column.strings.size());
    }
  }

  std::vector<VectorPtr> children;
  children.reserve(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    const auto &type = types[i];
    const auto &column = columns[i];
    if (column.kind == TypeKind::BIGINT) {
      auto buffer =
          AlignedBuffer::allocate(column.ints.size() * sizeof(int64_t), pool);
      std::memcpy(buffer->as_mutable_uint8_t(), column.ints.data(),
                  column.ints.size() * sizeof(int64_t));
      children.push_back(std::make_shared<FlatVector<int64_t>>(
          pool, type, nullptr, column.ints.size(), buffer));
    } else if (column.kind == TypeKind::INTEGER) {
      auto buffer =
          AlignedBuffer::allocate(column.int32s.size() * sizeof(int32_t), pool);
      std::memcpy(buffer->as_mutable_uint8_t(), column.int32s.data(),
                  column.int32s.size() * sizeof(int32_t));
      children.push_back(std::make_shared<FlatVector<int32_t>>(
          pool, type, nullptr, column.int32s.size(), buffer));
    } else if (column.kind == TypeKind::VARCHAR) {
      auto values = AlignedBuffer::allocate(
          column.strings.size() * sizeof(StringView), pool);
      auto *rawValues = values->asMutable<StringView>();
      size_t totalSize = 0;
      for (const auto &value : column.strings) {
        totalSize += value.size();
      }
      auto dataBuffer = AlignedBuffer::allocate(totalSize, pool);
      char *cursor = dataBuffer->asMutable<char>();
      size_t offset = 0;
      for (size_t row = 0; row < column.strings.size(); ++row) {
        const auto &value = column.strings[row];
        std::memcpy(cursor + offset, value.data(), value.size());
        rawValues[row] = StringView(cursor + offset, value.size());
        offset += value.size();
      }
      auto vector = std::make_shared<FlatVector<StringView>>(
          pool, type, nullptr, column.strings.size(), values);
      vector->addStringBuffer(dataBuffer);
      children.push_back(vector);
    } else {
      VELOX_FAIL("Unsupported type when building vectors");
    }
  }

  auto rowType = ROW(std::move(names), std::move(types));
  return std::make_shared<RowVector>(pool, rowType, nullptr, rowCount,
                                     std::move(children));
}

} // namespace facebook::velox::dwio::common
