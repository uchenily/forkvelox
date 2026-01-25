cmake_minimum_required(VERSION 3.20)
project(minivelox CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})



find_package(Threads REQUIRED)



# Define the library

add_library(velox_common

    velox/common/memory/Memory.cpp

    velox/expression/Expr.cpp

)



target_include_directories(velox_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})



add_executable(VeloxIn10MinDemo demo/VeloxIn10MinDemo.cpp)

target_link_libraries(VeloxIn10MinDemo velox_common Threads::Threads)
