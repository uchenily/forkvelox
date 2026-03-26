#include "velox/tpch/gen/TpchGen.h"

#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/StringView.h"
#include "velox/vector/FlatVector.h"

#include <cstring>
#include <fstream>
#include <string>

namespace facebook::velox::tpch {
namespace {

std::vector<std::string> split(const std::string& line, char delimiter) {
  std::vector<std::string> parts;
  size_t start = 0;
  while (start <= line.size()) {
    const auto end = line.find(delimiter, start);
    if (end == std::string::npos) {
      parts.push_back(line.substr(start));
      break;
    }
    parts.push_back(line.substr(start, end - start));
    start = end + 1;
  }
  return parts;
}

std::shared_ptr<FlatVector<int64_t>> makeIntVector(memory::MemoryPool* pool, const std::vector<int64_t>& values) {
  auto buffer = AlignedBuffer::allocate(values.size() * sizeof(int64_t), pool);
  std::memcpy(buffer->asMutable<uint8_t>(), values.data(), values.size() * sizeof(int64_t));
  return std::make_shared<FlatVector<int64_t>>(pool, BIGINT(), nullptr, values.size(), buffer);
}

std::shared_ptr<FlatVector<StringView>> makeStringVector(memory::MemoryPool* pool, const std::vector<std::string>& values) {
  auto buffer = AlignedBuffer::allocate(values.size() * sizeof(StringView), pool);
  auto* rawValues = buffer->asMutable<StringView>();
  size_t totalBytes = 0;
  for (const auto& value : values) {
    totalBytes += value.size();
  }
  auto stringBuffer = AlignedBuffer::allocate(totalBytes, pool);
  auto* rawChars = stringBuffer->asMutable<char>();
  size_t offset = 0;
  for (size_t i = 0; i < values.size(); ++i) {
    std::memcpy(rawChars + offset, values[i].data(), values[i].size());
    rawValues[i] = StringView(rawChars + offset, values[i].size());
    offset += values[i].size();
  }
  auto vector = std::make_shared<FlatVector<StringView>>(pool, VARCHAR(), nullptr, values.size(), buffer);
  vector->addStringBuffer(stringBuffer);
  return vector;
}

RowVectorPtr makeNation(memory::MemoryPool* pool) {
  const std::vector<int64_t> nationKeys = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
                                           13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24};
  const std::vector<std::string> names = {"ALGERIA",      "ARGENTINA",  "BRAZIL",  "CANADA",         "EGYPT",
                                          "ETHIOPIA",     "FRANCE",     "GERMANY", "INDIA",          "INDONESIA",
                                          "IRAN",         "IRAQ",       "JAPAN",   "JORDAN",         "KENYA",
                                          "MOROCCO",      "MOZAMBIQUE", "PERU",    "CHINA",          "ROMANIA",
                                          "SAUDI ARABIA", "VIETNAM",    "RUSSIA",  "UNITED KINGDOM", "UNITED STATES"};
  const std::vector<int64_t> regionKeys = {0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 4, 0, 1, 2, 3, 4, 2, 3, 3, 1};

  return std::make_shared<RowVector>(
      pool,
      ROW({"n_nationkey", "n_name", "n_regionkey"}, {BIGINT(), VARCHAR(), BIGINT()}),
      nullptr,
      nationKeys.size(),
      std::vector<VectorPtr>{makeIntVector(pool, nationKeys), makeStringVector(pool, names), makeIntVector(pool, regionKeys)});
}

RowVectorPtr makeRegion(memory::MemoryPool* pool) {
  const std::vector<int64_t> regionKeys = {0, 1, 2, 3, 4};
  const std::vector<std::string> names = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};

  return std::make_shared<RowVector>(
      pool,
      ROW({"r_regionkey", "r_name"}, {BIGINT(), VARCHAR()}),
      nullptr,
      regionKeys.size(),
      std::vector<VectorPtr>{makeIntVector(pool, regionKeys), makeStringVector(pool, names)});
}

int64_t parseCents(const std::string& text) {
  auto dot = text.find('.');
  if (dot == std::string::npos) {
    return std::stoll(text) * 100;
  }
  const auto whole = std::stoll(text.substr(0, dot));
  auto fraction = text.substr(dot + 1);
  while (fraction.size() < 2) {
    fraction.push_back('0');
  }
  if (fraction.size() > 2) {
    fraction.resize(2);
  }
  return whole * 100 + std::stoll(fraction);
}

int64_t parseDateKey(const std::string& text) {
  std::string normalized;
  normalized.reserve(text.size());
  for (char ch : text) {
    if (ch != '-') {
      normalized.push_back(ch);
    }
  }
  return std::stoll(normalized);
}

RowVectorPtr makeLineitem(memory::MemoryPool* pool, int scaleFactor) {
  VELOX_CHECK_EQ(scaleFactor, 1, "Only TPCH SF1 is supported right now");

  const std::vector<std::string> paths = {
      "thirdparties/tpch-kit/ref_data/1/lineitem.tbl.1",
      "thirdparties/tpch-kit/ref_data/1/lineitem.tbl.19999",
      "thirdparties/tpch-kit/ref_data/1/lineitem.tbl.39998",
      "thirdparties/tpch-kit/ref_data/1/lineitem.tbl.59997",
      "thirdparties/tpch-kit/ref_data/1/lineitem.tbl.60000",
  };

  std::vector<int64_t> quantities;
  std::vector<int64_t> discounts;
  std::vector<int64_t> shipdates;
  std::vector<int64_t> revenues;

  for (const auto& path : paths) {
    std::ifstream input(path);
    VELOX_CHECK(input.good(), "Failed to open TPCH lineitem data at {}", path);

    std::string line;
    while (std::getline(input, line)) {
      if (line.empty()) {
        continue;
      }
      auto columns = split(line, '|');
      VELOX_CHECK_GE(columns.size(), 11, "Unexpected TPCH lineitem row format");

      const int64_t quantity = std::stoll(columns[4]);
      const int64_t extendedPriceCents = parseCents(columns[5]);
      const int64_t discount = std::stoll(columns[6].substr(2));
      const int64_t shipdate = parseDateKey(columns[10]);
      const int64_t revenue = extendedPriceCents * discount;

      quantities.push_back(quantity);
      discounts.push_back(discount);
      shipdates.push_back(shipdate);
      revenues.push_back(revenue);
    }
  }

  return std::make_shared<RowVector>(
      pool,
      ROW({"l_quantity", "l_discount", "l_shipdate", "l_revenue"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()}),
      nullptr,
      quantities.size(),
      std::vector<VectorPtr>{
          makeIntVector(pool, quantities),
          makeIntVector(pool, discounts),
          makeIntVector(pool, shipdates),
          makeIntVector(pool, revenues)});
}

RowVectorPtr projectColumns(memory::MemoryPool* pool, const RowVectorPtr& data, const std::vector<std::string>& columns) {
  if (columns.empty()) {
    return data;
  }

  auto rowType = asRowType(data->type());
  std::vector<VectorPtr> children;
  std::vector<std::string> names;
  std::vector<TypePtr> types;

  for (const auto& column : columns) {
    bool found = false;
    for (size_t i = 0; i < rowType->size(); ++i) {
      if (rowType->nameOf(i) == column) {
        children.push_back(data->childAt(i));
        names.push_back(column);
        types.push_back(rowType->childAt(i));
        found = true;
        break;
      }
    }
    VELOX_CHECK(found, "Unknown TPCH column {}", column);
  }

  return std::make_shared<RowVector>(pool, ROW(names, types), nullptr, data->size(), children);
}

} // namespace

RowTypePtr getTableSchema(Table table) {
  switch (table) {
    case TBL_NATION:
      return ROW({"n_nationkey", "n_name", "n_regionkey"}, {BIGINT(), VARCHAR(), BIGINT()});
    case TBL_REGION:
      return ROW({"r_regionkey", "r_name"}, {BIGINT(), VARCHAR()});
    case TBL_LINEITEM:
      return ROW({"l_quantity", "l_discount", "l_shipdate", "l_revenue"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  }
  VELOX_FAIL("Unknown TPCH table");
}

RowVectorPtr readTable(memory::MemoryPool* pool, Table table, const std::vector<std::string>& columns, int scaleFactor) {
  RowVectorPtr data;
  switch (table) {
    case TBL_NATION:
      data = makeNation(pool);
      break;
    case TBL_REGION:
      data = makeRegion(pool);
      break;
    case TBL_LINEITEM:
      data = makeLineitem(pool, scaleFactor);
      break;
  }
  return projectColumns(pool, data, columns);
}

} // namespace facebook::velox::tpch
