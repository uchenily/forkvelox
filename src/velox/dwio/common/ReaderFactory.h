#pragma once

#include "velox/dwio/common/Reader.h"

namespace facebook::velox::dwio::common {

ReaderFactory *getReaderFactory(FileFormat format);
void registerReaderFactory(FileFormat format, std::unique_ptr<ReaderFactory> factory);

} // namespace facebook::velox::dwio::common
