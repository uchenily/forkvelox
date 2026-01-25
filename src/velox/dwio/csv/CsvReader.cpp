#include "velox/dwio/csv/CsvReader.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <sstream>
#include <utility>
#include <vector>

#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::dwio::csv {
namespace {

std::string trim(std::string value) {
  auto notSpace = [](unsigned char c) { return !std::isspace(c); };
  value.erase(value.begin(),
              std::find_if(value.begin(), value.end(), notSpace));
  value.erase(std::find_if(value.rbegin(), value.rend(), notSpace).base(),
              value.end());
  return value;
}

std::vector<std::string> splitCsv(const std::string& line, char delimiter) {
  std::vector<std::string> fields;
  std::string field;
  bool inQuotes = false;
  for (size_t i = 0; i < line.size(); ++i) {
    char c = line[i];
    if (c == '"') {
      if (inQuotes && i + 1 < line.size() && line[i + 1] == '"') {
        field.push_back('"');
        ++i;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (c == delimiter && !inQuotes) {
      fields.push_back(trim(field));
      field.clear();
    } else {
      field.push_back(c);
    }
  }
  fields.push_back(trim(field));
  return fields;
}

TypePtr parseType(const std::string& value) {
  if (value == "BIGINT") {
    return BIGINT();
  }
  if (value == "INTEGER") {
    return INTEGER();
  }
  if (value == "VARCHAR") {
    return VARCHAR();
  }
  VELOX_FAIL("Unsupported type: {}", value);
}

RowVectorPtr buildRowVector(
    memory::MemoryPool* pool,
    const RowTypePtr& rowType,
    const std::vector<std::vector<int64_t>>& int64Columns,
    const std::vector<std::vector<int32_t>>& int32Columns,
    const std::vector<std::vector<std::string>>& stringColumns,
    const std::vector<TypeKind>& columnKinds) {
  std::vector<VectorPtr> children;
  children.reserve(rowType->size());

  for (size_t i = 0; i < rowType->size(); ++i) {
    if (columnKinds[i] == TypeKind::BIGINT) {
      const auto& data = int64Columns[i];
      auto buffer = AlignedBuffer::allocate(data.size() * sizeof(int64_t), pool);
      std::memcpy(
          buffer->as_mutable_uint8_t(),
          data.data(),
          data.size() * sizeof(int64_t));
      children.push_back(std::make_shared<FlatVector<int64_t>>(
          pool, rowType->childAt(i), nullptr, data.size(), buffer));
    } else if (columnKinds[i] == TypeKind::INTEGER) {
      const auto& data = int32Columns[i];
      auto buffer = AlignedBuffer::allocate(data.size() * sizeof(int32_t), pool);
      std::memcpy(
          buffer->as_mutable_uint8_t(),
          data.data(),
          data.size() * sizeof(int32_t));
      children.push_back(std::make_shared<FlatVector<int32_t>>(
          pool, rowType->childAt(i), nullptr, data.size(), buffer));
    } else if (columnKinds[i] == TypeKind::VARCHAR) {
      const auto& data = stringColumns[i];
      auto values =
          AlignedBuffer::allocate(data.size() * sizeof(StringView), pool);
      auto* rawValues = values->asMutable<StringView>();
      size_t totalSize = 0;
      for (const auto& value : data) {
        totalSize += value.size();
      }
      auto dataBuffer = AlignedBuffer::allocate(totalSize, pool);
      char* cursor = dataBuffer->asMutable<char>();
      size_t offset = 0;
      for (size_t row = 0; row < data.size(); ++row) {
        const auto& value = data[row];
        std::memcpy(cursor + offset, value.data(), value.size());
        rawValues[row] = StringView(cursor + offset, value.size());
        offset += value.size();
      }
      auto vector = std::make_shared<FlatVector<StringView>>(
          pool, rowType->childAt(i), nullptr, data.size(), values);
      vector->addStringBuffer(dataBuffer);
      children.push_back(vector);
    } else {
      VELOX_FAIL("Unsupported type when building vectors");
    }
  }

  vector_size_t rowCount = 0;
  if (!children.empty()) {
    rowCount = children[0]->size();
  }
  return std::make_shared<RowVector>(
      pool, rowType, nullptr, rowCount, std::move(children));
}

} // namespace

CsvReader::CsvReader(
    std::shared_ptr<ReadFile> file,
    memory::MemoryPool* pool,
    CsvReadOptions options)
    : file_(std::move(file)), pool_(pool), options_(options) {
  VELOX_CHECK(file_ != nullptr, "ReadFile must not be null");
  auto input = file_->open();

  std::string line;
  if (options_.hasHeader) {
    VELOX_CHECK(
        std::getline(*input, line),
        "Missing header line in {}",
        file_->path());
  } else {
    VELOX_FAIL("CSV reader requires header line with column names");
  }
  auto names = splitCsv(line, options_.delimiter);

  std::vector<TypePtr> types;
  if (options_.hasTypes) {
    VELOX_CHECK(
        std::getline(*input, line),
        "Missing types line in {}",
        file_->path());
    auto typeNames = splitCsv(line, options_.delimiter);
    bool allTypes = typeNames.size() == names.size();
    if (allTypes) {
      for (const auto& typeName : typeNames) {
        try {
          types.push_back(parseType(typeName));
        } catch (const VeloxException&) {
          allTypes = false;
          types.clear();
          break;
        }
      }
    }
    if (!allTypes) {
      if (options_.inferTypesIfMissing) {
        types.assign(names.size(), VARCHAR());
        options_.hasTypes = false;
        firstDataLine_ = line;
        hasFirstDataLine_ = true;
      } else {
        VELOX_FAIL("Missing or invalid types line in {}", file_->path());
      }
    }
  } else {
    types.assign(names.size(), VARCHAR());
  }

  rowType_ = ROW(std::move(names), std::move(types));
}

std::unique_ptr<CsvRowReader> CsvReader::createRowReader() const {
  return std::make_unique<CsvRowReader>(
      file_, rowType_, pool_, options_, firstDataLine_, hasFirstDataLine_);
}

CsvRowReader::CsvRowReader(
    std::shared_ptr<ReadFile> file,
    RowTypePtr rowType,
    memory::MemoryPool* pool,
    CsvReadOptions options,
    std::string firstDataLine,
    bool hasFirstDataLine)
    : file_(std::move(file)),
      rowType_(std::move(rowType)),
      pool_(pool),
      options_(options),
      firstDataLine_(std::move(firstDataLine)),
      hasFirstDataLine_(hasFirstDataLine) {}

void CsvRowReader::ensureInitialized() {
  if (initialized_) {
    return;
  }
  VELOX_CHECK(file_ != nullptr, "ReadFile must not be null");
  input_ = file_->open();
  std::string line;
  if (options_.hasHeader) {
    VELOX_CHECK(
        std::getline(*input_, line),
        "Missing header line in {}",
        file_->path());
  }
  if (options_.hasTypes) {
    VELOX_CHECK(
        std::getline(*input_, line),
        "Missing types line in {}",
        file_->path());
  }
  initialized_ = true;
}

bool CsvRowReader::next(size_t batchSize, RowVectorPtr& out) {
  if (eof_) {
    return false;
  }
  ensureInitialized();

  const auto columnCount = rowType_->size();
  std::vector<TypeKind> columnKinds;
  columnKinds.reserve(columnCount);
  for (size_t i = 0; i < columnCount; ++i) {
    columnKinds.push_back(rowType_->childAt(i)->kind());
  }

  std::vector<std::vector<int64_t>> int64Columns(columnCount);
  std::vector<std::vector<int32_t>> int32Columns(columnCount);
  std::vector<std::vector<std::string>> stringColumns(columnCount);

  size_t rows = 0;
  std::string line;
  if (hasFirstDataLine_) {
    line = firstDataLine_;
    hasFirstDataLine_ = false;
  }
  while (rows < batchSize &&
         (!line.empty() || std::getline(*input_, line))) {
    if (line.empty()) {
      line.clear();
      continue;
    }
    auto values = splitCsv(line, options_.delimiter);
    VELOX_CHECK(
        values.size() == columnCount,
        "Row column count mismatch");
    for (size_t i = 0; i < columnCount; ++i) {
      auto value = values[i];
      if (columnKinds[i] == TypeKind::BIGINT) {
        int64Columns[i].push_back(std::stoll(value));
      } else if (columnKinds[i] == TypeKind::INTEGER) {
        int32Columns[i].push_back(static_cast<int32_t>(std::stol(value)));
      } else if (columnKinds[i] == TypeKind::VARCHAR) {
        stringColumns[i].push_back(value);
      } else {
        VELOX_FAIL("Unsupported type in CSV data");
      }
    }
    ++rows;
    line.clear();
  }

  if (rows == 0) {
    eof_ = true;
    return false;
  }

  out = buildRowVector(
      pool_, rowType_, int64Columns, int32Columns, stringColumns, columnKinds);
  return true;
}

} // namespace facebook::velox::dwio::csv
