add_library(velox_common
    velox/vector/BaseVector.cpp
    velox/common/memory/Memory.cpp
    velox/common/base/Log.cpp
    velox/common/dynamic_registry/DynamicLibraryLoader.cpp
    velox/common/file/FileSystems.cpp
    velox/common/file/File.cpp
    velox/dwio/common/RowVectorFile.cpp
    velox/dwio/csv/CsvReader.cpp
    velox/dwio/fvx/FvxReader.cpp
    velox/dwio/fvx/FvxWriter.cpp
    velox/dwio/common/ReaderFactory.cpp
    velox/dwio/csv/RegisterCsvReader.cpp
    velox/dwio/fvx/RegisterFvxReader.cpp
    velox/exec/LocalExchange.cpp
    velox/exec/LocalPlanner.cpp
    velox/exec/PlanNodeStats.cpp
    velox/exec/Task.cpp
    velox/expression/Expr.cpp
    velox/expression/LambdaExpr.cpp
    velox/expression/FunctionRegistry.cpp
    velox/functions/aggregates/AggregateFunction.cpp
    velox/functions/aggregates/RegisterAggregateFunctions.cpp
    velox/functions/Arithmetic.cpp
    velox/functions/LambdaFunctions.cpp
    velox/functions/StringFunctions.cpp
    velox/functions/Comparison.cpp
    velox/functions/registration/RegistrationFunctions.cpp
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(velox_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
if (UNIX)
  target_link_libraries(velox_common PUBLIC dl)
endif()
set_target_properties(velox_common PROPERTIES POSITION_INDEPENDENT_CODE ON)
