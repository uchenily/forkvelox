#include "velox/tpch/gen/TpchGen.h"

#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/StringView.h"
#include "velox/vector/FlatVector.h"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_set>

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

std::shared_ptr<FlatVector<double>> makeDoubleVector(memory::MemoryPool* pool, const std::vector<double>& values) {
  auto buffer = AlignedBuffer::allocate(values.size() * sizeof(double), pool);
  std::memcpy(buffer->asMutable<uint8_t>(), values.data(), values.size() * sizeof(double));
  return std::make_shared<FlatVector<double>>(pool, DOUBLE(), nullptr, values.size(), buffer);
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

double parseDouble(const std::string& text) {
  return std::stod(text);
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

bool wants(const std::unordered_set<std::string>& requested, const std::string& column) {
  return requested.empty() || requested.count(column) > 0;
}

RowVectorPtr makeLineitem(memory::MemoryPool* pool, int scaleFactor, const std::vector<std::string>& columnsToKeep) {
  VELOX_CHECK_EQ(scaleFactor, 1, "Only TPCH SF1 is supported right now");
  const std::string path = std::string(VELOX_TPCH_SF1_DIR) + "/lineitem.tbl";
  const std::unordered_set<std::string> requested(columnsToKeep.begin(), columnsToKeep.end());

  std::vector<double> quantities;
  std::vector<double> extendedPrices;
  std::vector<double> discounts;
  std::vector<double> taxes;
  std::vector<std::string> returnFlags;
  std::vector<std::string> lineStatuses;
  std::vector<int64_t> shipdates;
  std::vector<double> revenues;
  std::vector<double> discountedPrices;
  std::vector<double> charges;

  std::ifstream input(path);
  VELOX_CHECK(
      input.good(),
      "Failed to open generated TPCH lineitem data at {}. Build target `TpchQ6` to generate SF1 data with dbgen.",
      path);

  std::string line;
  while (std::getline(input, line)) {
    if (line.empty()) {
      continue;
    }
    auto columns = split(line, '|');
    VELOX_CHECK_GE(columns.size(), 11, "Unexpected TPCH lineitem row format");

    const double quantity = parseDouble(columns[4]);
    const double extendedPrice = parseDouble(columns[5]);
    const double discount = parseDouble(columns[6]);
    const double tax = parseDouble(columns[7]);
    const int64_t shipdate = parseDateKey(columns[10]);
    const double discountedPrice = extendedPrice * (1.0 - discount);
    const double charge = discountedPrice * (1.0 + tax);
    const double revenue = extendedPrice * discount;

    quantities.push_back(quantity);
    extendedPrices.push_back(extendedPrice);
    discounts.push_back(discount);
    taxes.push_back(tax);
    returnFlags.push_back(columns[8]);
    lineStatuses.push_back(columns[9]);
    shipdates.push_back(shipdate);
    revenues.push_back(revenue);
    discountedPrices.push_back(discountedPrice);
    charges.push_back(charge);
  }

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<VectorPtr> children;
  if (wants(requested, "l_quantity")) {
    names.push_back("l_quantity");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, quantities));
  }
  if (wants(requested, "l_extendedprice")) {
    names.push_back("l_extendedprice");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, extendedPrices));
  }
  if (wants(requested, "l_discount")) {
    names.push_back("l_discount");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, discounts));
  }
  if (wants(requested, "l_tax")) {
    names.push_back("l_tax");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, taxes));
  }
  if (wants(requested, "l_returnflag")) {
    names.push_back("l_returnflag");
    types.push_back(VARCHAR());
    children.push_back(makeStringVector(pool, returnFlags));
  }
  if (wants(requested, "l_linestatus")) {
    names.push_back("l_linestatus");
    types.push_back(VARCHAR());
    children.push_back(makeStringVector(pool, lineStatuses));
  }
  if (wants(requested, "l_shipdate")) {
    names.push_back("l_shipdate");
    types.push_back(BIGINT());
    children.push_back(makeIntVector(pool, shipdates));
  }
  if (wants(requested, "l_revenue")) {
    names.push_back("l_revenue");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, revenues));
  }
  if (wants(requested, "l_disc_price")) {
    names.push_back("l_disc_price");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, discountedPrices));
  }
  if (wants(requested, "l_charge")) {
    names.push_back("l_charge");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, charges));
  }

  return std::make_shared<RowVector>(pool, ROW(names, types), nullptr, quantities.size(), children);
}

RowVectorPtr makeLineitemSplit(
    memory::MemoryPool* pool,
    int scaleFactor,
    int part,
    int totalParts,
    const std::vector<std::string>& columnsToKeep) {
  VELOX_CHECK_EQ(scaleFactor, 1, "Only TPCH SF1 is supported right now");
  VELOX_CHECK_GE(part, 0, "TPCH split part must be non-negative");
  VELOX_CHECK_GT(totalParts, 0, "TPCH split totalParts must be positive");
  VELOX_CHECK_LT(part, totalParts, "TPCH split part must be less than totalParts");

  const std::string path = std::string(VELOX_TPCH_SF1_DIR) + "/lineitem.tbl";
  VELOX_CHECK(std::filesystem::exists(path), "Generated TPCH lineitem data not found at {}", path);
  const std::unordered_set<std::string> requested(columnsToKeep.begin(), columnsToKeep.end());

  const auto fileSize = static_cast<std::streamoff>(std::filesystem::file_size(path));
  const auto start = (fileSize * part) / totalParts;
  const auto end = (fileSize * (part + 1)) / totalParts;

  // FIXME: use async api
  std::ifstream input(path);
  VELOX_CHECK(input.good(), "Failed to open generated TPCH lineitem data at {}", path);

  if (start > 0) {
    input.seekg(start - 1);
    char ch = 0;
    while (input.get(ch)) {
      if (ch == '\n') {
        break;
      }
    }
  }

  std::vector<double> quantities;
  std::vector<double> extendedPrices;
  std::vector<double> discounts;
  std::vector<double> taxes;
  std::vector<std::string> returnFlags;
  std::vector<std::string> lineStatuses;
  std::vector<int64_t> shipdates;
  std::vector<double> revenues;
  std::vector<double> discountedPrices;
  std::vector<double> charges;

  std::string line;
  while (true) {
    const auto position = input.tellg();
    if (position == std::streampos(-1)) {
      break;
    }
    if (position >= end && part + 1 < totalParts) {
      break;
    }
    if (!std::getline(input, line)) {
      break;
    }
    if (line.empty()) {
      continue;
    }

    auto columns = split(line, '|');
    VELOX_CHECK_GE(columns.size(), 11, "Unexpected TPCH lineitem row format");

    const double quantity = parseDouble(columns[4]);
    const double extendedPrice = parseDouble(columns[5]);
    const double discount = parseDouble(columns[6]);
    const double tax = parseDouble(columns[7]);
    const int64_t shipdate = parseDateKey(columns[10]);
    // FIXME: use expr
    const double discountedPrice = extendedPrice * (1.0 - discount);
    const double charge = discountedPrice * (1.0 + tax);
    const double revenue = extendedPrice * discount;

    quantities.push_back(quantity);
    extendedPrices.push_back(extendedPrice);
    discounts.push_back(discount);
    taxes.push_back(tax);
    returnFlags.push_back(columns[8]);
    lineStatuses.push_back(columns[9]);
    shipdates.push_back(shipdate);
    revenues.push_back(revenue);
    discountedPrices.push_back(discountedPrice);
    charges.push_back(charge);
  }

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<VectorPtr> children;
  if (wants(requested, "l_quantity")) {
    names.push_back("l_quantity");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, quantities));
  }
  if (wants(requested, "l_extendedprice")) {
    names.push_back("l_extendedprice");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, extendedPrices));
  }
  if (wants(requested, "l_discount")) {
    names.push_back("l_discount");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, discounts));
  }
  if (wants(requested, "l_tax")) {
    names.push_back("l_tax");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, taxes));
  }
  if (wants(requested, "l_returnflag")) {
    names.push_back("l_returnflag");
    types.push_back(VARCHAR());
    children.push_back(makeStringVector(pool, returnFlags));
  }
  if (wants(requested, "l_linestatus")) {
    names.push_back("l_linestatus");
    types.push_back(VARCHAR());
    children.push_back(makeStringVector(pool, lineStatuses));
  }
  if (wants(requested, "l_shipdate")) {
    names.push_back("l_shipdate");
    types.push_back(BIGINT());
    children.push_back(makeIntVector(pool, shipdates));
  }
  if (wants(requested, "l_revenue")) {
    names.push_back("l_revenue");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, revenues));
  }
  if (wants(requested, "l_disc_price")) {
    names.push_back("l_disc_price");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, discountedPrices));
  }
  if (wants(requested, "l_charge")) {
    names.push_back("l_charge");
    types.push_back(DOUBLE());
    children.push_back(makeDoubleVector(pool, charges));
  }

  return std::make_shared<RowVector>(pool, ROW(names, types), nullptr, quantities.size(), children);
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
      return ROW({"l_quantity",
                  "l_extendedprice",
                  "l_discount",
                  "l_tax",
                  "l_returnflag",
                  "l_linestatus",
                  "l_shipdate",
                  "l_revenue",
                  "l_disc_price",
                  "l_charge"},
                 {DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), VARCHAR(), VARCHAR(), BIGINT(), DOUBLE(), DOUBLE(), DOUBLE()});
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
      data = makeLineitem(pool, scaleFactor, columns);
      break;
  }
  return projectColumns(pool, data, columns);
}

RowVectorPtr readTableSplit(
    memory::MemoryPool* pool,
    Table table,
    const std::vector<std::string>& columns,
    int scaleFactor,
    int part,
    int totalParts) {
  RowVectorPtr data;
  switch (table) {
    case TBL_NATION:
      VELOX_CHECK_EQ(part, 0, "Nation table supports a single split");
      data = makeNation(pool);
      break;
    case TBL_REGION:
      VELOX_CHECK_EQ(part, 0, "Region table supports a single split");
      data = makeRegion(pool);
      break;
    case TBL_LINEITEM:
      data = makeLineitemSplit(pool, scaleFactor, part, totalParts, columns);
      break;
  }
  return projectColumns(pool, data, columns);
}

} // namespace facebook::velox::tpch
