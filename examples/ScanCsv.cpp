#include <folly/init/Init.h>
#include <iostream>

#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/csv/RegisterCsvReader.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

using namespace facebook::velox;

// A temporary program that reads from a CSV file and prints its content.
// CSV format:
//   1) Header line: col_name,col_name,...
//   2) Optional types line: BIGINT,INTEGER,VARCHAR,...
//   3) Data lines: value,value,...
// Usage: ScanCsvDemo [csv_file_path]
int main(int argc, char **argv) {
  folly::init::Init init{&argc, &argv, false};

  filesystems::registerLocalFileSystem();
  dwio::csv::registerCsvReaderFactory();
  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  auto pool = memory::defaultMemoryPool();

  const std::string filePath = (argc >= 2) ? std::string{argv[1]} : std::string{"data/sample.csv"};
  dwio::common::ReaderOptions readerOpts{pool.get()};
  readerOpts.setFileFormat(dwio::common::FileFormat::CSV);

  auto reader = dwio::common::getReaderFactory(dwio::common::FileFormat::CSV)
                    ->createReader(std::make_unique<dwio::common::BufferedInput>(
                                       std::make_shared<LocalReadFile>(filePath), readerOpts.memoryPool()),
                                   readerOpts);

  VectorPtr batch;
  dwio::common::RowReaderOptions rowReaderOptions;
  auto rowReader = reader->createRowReader(rowReaderOptions);
  while (rowReader->next(500, batch)) {
    auto rowVector = std::dynamic_pointer_cast<RowVector>(batch);
    for (vector_size_t i = 0; i < rowVector->size(); ++i) {
      std::cout << rowVector->toString(i) << std::endl;
    }
  }

  return 0;
}
