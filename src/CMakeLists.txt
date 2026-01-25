# Define the library
add_library(velox_common
    velox/common/memory/Memory.cpp
    velox/io/RowVectorFile.cpp
    velox/expression/Expr.cpp
    velox/expression/FunctionRegistry.cpp
    velox/functions/prestosql/Arithmetic.cpp
    velox/functions/prestosql/StringFunctions.cpp
    velox/functions/prestosql/Comparison.cpp
    velox/functions/prestosql/registration/RegistrationFunctions.cpp
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(velox_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(VeloxIn10MinDemo demo/VeloxIn10MinDemo.cpp)
target_link_libraries(VeloxIn10MinDemo velox_common Threads::Threads)

add_executable(ScanAndSortDemo demo/ScanAndSortDemo.cpp)
target_link_libraries(ScanAndSortDemo velox_common Threads::Threads)
