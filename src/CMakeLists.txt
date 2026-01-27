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

add_subdirectory(examples)
