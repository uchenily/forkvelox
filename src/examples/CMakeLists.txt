add_executable(VeloxIn10Min VeloxIn10Min.cpp)
target_link_libraries(VeloxIn10Min velox_common Threads::Threads)

add_executable(ScanAndSort ScanAndSort.cpp)
target_link_libraries(ScanAndSort velox_common Threads::Threads)

add_executable(ScanCsv ScanCsv.cpp)
target_link_libraries(ScanCsv velox_common Threads::Threads)

add_executable(TaskParallel TaskParallel.cpp)
target_link_libraries(TaskParallel velox_common Threads::Threads)

add_executable(PipelineSplit PipelineSplit.cpp)
target_link_libraries(PipelineSplit velox_common Threads::Threads)

add_executable(TwoHashJoin TwoHashJoin.cpp)
target_link_libraries(TwoHashJoin velox_common Threads::Threads)

add_executable(OrderByPartialFinal OrderByPartialFinal.cpp)
target_link_libraries(OrderByPartialFinal velox_common Threads::Threads)

add_executable(MultiSplitScan MultiSplitScan.cpp)
target_link_libraries(MultiSplitScan velox_common Threads::Threads)

add_executable(AggregationStep AggregationStep.cpp)
target_link_libraries(AggregationStep velox_common Threads::Threads)

add_executable(VectorReaderWriter VectorReaderWriter.cpp)
target_link_libraries(VectorReaderWriter velox_common Threads::Threads)

add_executable(CancellationRequeue CancellationRequeue.cpp)
target_link_libraries(CancellationRequeue velox_common Threads::Threads)
