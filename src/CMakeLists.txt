add_library(velox_common
    velox/common/memory/Memory.cpp
    velox/common/file/FileSystems.cpp
    velox/common/file/File.cpp
    velox/dwio/common/RowVectorFile.cpp
    velox/dwio/csv/CsvReader.cpp
    velox/dwio/common/ReaderFactory.cpp
    velox/dwio/csv/RegisterCsvReader.cpp
    velox/exec/LocalExchange.cpp
    velox/exec/LocalPlanner.cpp
    velox/exec/Task.cpp
    velox/expression/Expr.cpp
    velox/expression/FunctionRegistry.cpp
    velox/functions/prestosql/Arithmetic.cpp
    velox/functions/prestosql/StringFunctions.cpp
    velox/functions/prestosql/Comparison.cpp
    velox/functions/prestosql/registration/RegistrationFunctions.cpp
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(velox_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(VeloxIn10MinDemo examples/VeloxIn10MinDemo.cpp)
target_link_libraries(VeloxIn10MinDemo velox_common Threads::Threads)

add_executable(ScanAndSortDemo examples/ScanAndSortDemo.cpp)
target_link_libraries(ScanAndSortDemo velox_common Threads::Threads)

add_executable(ScanCsvDemo examples/ScanCsvDemo.cpp)
target_link_libraries(ScanCsvDemo velox_common Threads::Threads)

add_executable(TaskParallelDemo examples/TaskParallelDemo.cpp)
target_link_libraries(TaskParallelDemo velox_common Threads::Threads)

add_executable(PipelineSplitDemo examples/PipelineSplitDemo.cpp)
target_link_libraries(PipelineSplitDemo velox_common Threads::Threads)

add_executable(TwoHashJoinDemo examples/TwoHashJoinDemo.cpp)
target_link_libraries(TwoHashJoinDemo velox_common Threads::Threads)

add_executable(OrderByPartialFinalDemo examples/OrderByPartialFinalDemo.cpp)
target_link_libraries(OrderByPartialFinalDemo velox_common Threads::Threads)

add_executable(MultiSplitScanDemo examples/MultiSplitScanDemo.cpp)
target_link_libraries(MultiSplitScanDemo velox_common Threads::Threads)

add_executable(AggregationStepDemo examples/AggregationStepDemo.cpp)
target_link_libraries(AggregationStepDemo velox_common Threads::Threads)

add_executable(VectorReaderWriterDemo examples/VectorReaderWriterDemo.cpp)
target_link_libraries(VectorReaderWriterDemo velox_common Threads::Threads)

add_executable(CancellationRequeueDemo examples/CancellationRequeueDemo.cpp)
target_link_libraries(CancellationRequeueDemo velox_common Threads::Threads)
