#pragma once

#include <deque>
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
    struct Page {
      uint32_t rowCount{0};
      uint32_t startRow{0};
      uint64_t offset{0};
      uint64_t length{0};
      uint32_t uncompressedSize{0};
      uint32_t compressedSize{0};
      ColumnStats stats;
    };

    uint64_t offset{0};
    uint64_t length{0};
    ColumnStats stats;
    std::vector<Page> pages;
  };

  struct RowGroup {
    uint32_t rowCount{0};
    std::vector<ColumnChunk> columns;
  };

  std::shared_ptr<ReadFile> file_;
  memory::MemoryPool *pool_;
  uint64_t fileSize_{0};
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
  struct PreparedFilter {
    size_t columnIndex{0};
    dwio::common::CompareOp op{dwio::common::CompareOp::EQ};
    Variant value;
  };

  struct ColumnBuffer {
    TypeKind kind{TypeKind::UNKNOWN};
    std::vector<int64_t> int64s;
    std::vector<int32_t> int32s;
    std::vector<std::string> strings;
  };

  struct RowGroupCache {
    const FvxReader::RowGroup *rowGroup{nullptr};
    std::vector<vector_size_t> selectedRows;
  };

  const FvxReader *reader_;
  dwio::common::RowReaderOptions options_;
  RowTypePtr outputType_;
  std::vector<size_t> projectedIndices_;
  std::vector<size_t> filterColumnIndices_;
  std::vector<PreparedFilter> filters_;
  std::vector<std::vector<size_t>> filterIndicesByColumn_;
  mutable std::unordered_map<uint64_t, std::string> pageDataCache_;
  mutable std::deque<uint64_t> pageDataCacheOrder_;
  size_t rowGroupIndex_{0};
  vector_size_t rowOffsetInGroup_{0};
  std::optional<RowGroupCache> currentGroup_;
  static constexpr size_t kPageDataCacheCapacity = 64;

  bool loadNextMatchingRowGroup();
  bool rowGroupMatches(const FvxReader::RowGroup &rowGroup) const;
  bool columnMayMatch(const FvxReader::ColumnStats &stats, dwio::common::CompareOp op, const Variant &value) const;
  bool columnMatches(const ColumnBuffer &column, vector_size_t row, dwio::common::CompareOp op,
                     const Variant &value) const;
  std::vector<size_t> collectMatchingPages(const FvxReader::ColumnChunk &chunk, size_t columnIndex) const;
  std::vector<vector_size_t> collectRowsForMatchingPages(const FvxReader::ColumnChunk &chunk,
                                                         const std::vector<size_t> &matchingPages,
                                                         const std::vector<vector_size_t> &candidateRows) const;
  std::string_view readPageDataCached(const FvxReader::ColumnChunk::Page &page) const;
  void clearPageDataCache() const;

  RowVectorPtr buildRowVectorFromCache(const RowGroupCache &cache, const vector_size_t *rows, vector_size_t count) const;
  ColumnBuffer decodeColumnSelective(const FvxReader::ColumnChunk &chunk, TypeKind kind, const vector_size_t *rows,
                                     vector_size_t count, const std::vector<size_t> *pages) const;
  void buildProjection();
  void buildFiltersAndRequiredColumns();
};

} // namespace facebook::velox::dwio::fvx
