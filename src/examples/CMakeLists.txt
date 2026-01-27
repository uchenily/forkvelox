set(example_targets
  VeloxIn10Min
  ScanAndSort
  ScanCsv
  TaskParallel
  PipelineSplit
  TwoHashJoin
  OrderByPartialFinal
  MultiSplitScan
  AggregationStep
  VectorReaderWriter
  CancellationRequeue
)

foreach(target_name IN LISTS example_targets)
  add_executable(${target_name} ${target_name}.cpp)
  target_link_libraries(${target_name} velox_common Threads::Threads)
endforeach()
