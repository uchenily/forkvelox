#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/common/file/File.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/dwio/common/Reader.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::dwio::fvx {

class FvxRowReader;

class FvxReader {
public:
  FvxReader(std::shared_ptr<ReadFile> file, memory::MemoryPool *pool);

  std::unique_ptr<FvxRowReader> createRowReader(const dwio::common::RowReaderOptions &options) const;

  const RowTypePtr &rowType() const { return rowType_; }

private:
  struct ColumnStats {
    TypeKind kind{TypeKind::UNKNOWN};
    bool hasMinMax{false};
    int64_t minBigint{0};
    int64_t maxBigint{0};
    int32_t minInt{0};
    int32_t maxInt{0};
    std::string minString;
    std::string maxString;
  };

  struct ColumnChunk {
    uint64_t offset{0};
    uint64_t length{0};
    ColumnStats stats;
  };

  struct RowGroup {
    uint32_t rowCount{0};
    std::vector<ColumnChunk> columns;
  };

  std::shared_ptr<ReadFile> file_;
  memory::MemoryPool *pool_;
  std::string data_;
  RowTypePtr rowType_;
  std::vector<RowGroup> rowGroups_;
  std::unordered_map<std::string, size_t> nameToIndex_;

  void parseFile();
  void buildNameToIndex();

  friend class FvxRowReader;
};

class FvxRowReader {
public:
  FvxRowReader(const FvxReader *reader, dwio::common::RowReaderOptions options);

  bool next(size_t batchSize, RowVectorPtr &out);

private:
  struct ColumnBuffer {
    TypeKind kind{TypeKind::UNKNOWN};
    std::vector<int64_t> int64s;
    std::vector<int32_t> int32s;
    std::vector<std::string> strings;
  };

  struct RowGroupCache {
    uint32_t rowCount{0};
    std::vector<ColumnBuffer> columns;
  };

  const FvxReader *reader_;
  dwio::common::RowReaderOptions options_;
  RowTypePtr outputType_;
  std::vector<size_t> projectedIndices_;
  size_t rowGroupIndex_{0};
  vector_size_t rowOffsetInGroup_{0};
  std::optional<RowGroupCache> currentGroup_;

  bool loadNextMatchingRowGroup();
  bool rowGroupMatches(const FvxReader::RowGroup &rowGroup) const;
  bool columnMayMatch(const FvxReader::ColumnStats &stats, dwio::common::CompareOp op, const Variant &value) const;

  RowVectorPtr buildRowVectorFromCache(const RowGroupCache &cache, vector_size_t offset, vector_size_t count) const;
  ColumnBuffer decodeColumn(const FvxReader::ColumnChunk &chunk, TypeKind kind, uint32_t rowCount) const;
  void buildProjection();
};

} // namespace facebook::velox::dwio::fvx
