add_library(liburing_vendor STATIC
    ${CMAKE_SOURCE_DIR}/thirdparties/liburing/src/queue.c
    ${CMAKE_SOURCE_DIR}/thirdparties/liburing/src/register.c
    ${CMAKE_SOURCE_DIR}/thirdparties/liburing/src/setup.c
    ${CMAKE_SOURCE_DIR}/thirdparties/liburing/src/syscall.c
    ${CMAKE_SOURCE_DIR}/thirdparties/liburing/src/version.c
)
target_include_directories(liburing_vendor PUBLIC
    ${CMAKE_SOURCE_DIR}/thirdparties/liburing/src/include
)
target_include_directories(liburing_vendor PRIVATE
    ${CMAKE_SOURCE_DIR}/thirdparties/liburing/src
)

add_library(stdexec_vendor INTERFACE)
target_include_directories(stdexec_vendor INTERFACE
    ${CMAKE_SOURCE_DIR}/thirdparties/stdexec/include
)

set(TPCH_DBGEN_DIR ${CMAKE_SOURCE_DIR}/thirdparties/tpch-kit/dbgen)
set(TPCH_SF1_DIR ${CMAKE_BINARY_DIR}/tpch/sf1)
set(TPCH_SF1_LINEITEM ${TPCH_SF1_DIR}/lineitem.tbl)

add_custom_target(tpch_dbgen
    COMMAND make CC=gcc\ -std=gnu89 LIBS=-lm\ -pthread MACHINE=LINUX DATABASE=POSTGRESQL
    WORKING_DIRECTORY ${TPCH_DBGEN_DIR}
)

add_custom_command(
    OUTPUT ${TPCH_SF1_LINEITEM}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${TPCH_SF1_DIR}
    COMMAND ${CMAKE_COMMAND} -E env
        DSS_CONFIG=${TPCH_DBGEN_DIR}
        DSS_PATH=${TPCH_SF1_DIR}
        ${TPCH_DBGEN_DIR}/dbgen -f -s 1 -T L
    DEPENDS tpch_dbgen
    WORKING_DIRECTORY ${TPCH_DBGEN_DIR}
)

add_custom_target(tpch_sf1_lineitem DEPENDS ${TPCH_SF1_LINEITEM})

add_library(velox_common
    velox/vector/BaseVector.cpp
    velox/common/memory/Memory.cpp
    velox/common/base/Log.cpp
    velox/common/dynamic_registry/DynamicLibraryLoader.cpp
    velox/common/file/FileSystems.cpp
    velox/common/file/File.cpp
    velox/common/io/IoUringExecutor.cpp
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
    velox/core/ExecutionRuntime.cpp
    velox/tpch/gen/TpchGen.cpp
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
target_link_libraries(velox_common PUBLIC stdexec_vendor liburing_vendor)
target_compile_definitions(velox_common PUBLIC VELOX_TPCH_SF1_DIR="${TPCH_SF1_DIR}")
if (UNIX)
  target_link_libraries(velox_common PUBLIC dl)
endif()
set_target_properties(velox_common PROPERTIES POSITION_INDEPENDENT_CODE ON)
