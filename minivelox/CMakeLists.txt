cmake_minimum_required(VERSION 3.20)
project(minivelox)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Sources
set(COMMON_SOURCES
    common/memory/Memory.cpp
    type/Type.cpp
    buffer/Buffer.cpp
    vector/BaseVector.cpp
    vector/SimpleVector.cpp
    vector/RowVector.cpp
    exec/Expr.cpp
    # exec/Operator.cpp
    # exec/PlanNode.cpp
    # exec/Driver.cpp
    # exec/Task.cpp
    # exec/PlanBuilder.cpp
)

add_executable(test_build test_build.cpp ${COMMON_SOURCES})
target_link_libraries(test_build PRIVATE -pthread)

add_executable(velox_demo demo/VeloxIn10MinDemo.cpp ${COMMON_SOURCES})
target_link_libraries(velox_demo PRIVATE -pthread)

# Compile options for strictness/safety if desired, but keeping simple for now.
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  target_compile_options(velox_demo PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()