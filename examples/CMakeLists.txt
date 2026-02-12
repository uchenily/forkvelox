file(GLOB example_sources
  "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)
list(SORT example_sources)
set(example_targets)

add_library(velox_dynamic_example SHARED
  "${CMAKE_CURRENT_SOURCE_DIR}/dynamic/DynamicFunction.cpp"
)
target_include_directories(velox_dynamic_example PRIVATE ${CMAKE_SOURCE_DIR}/src)

foreach(source_file IN LISTS example_sources)
  get_filename_component(target_name "${source_file}" NAME_WE)
  add_executable(${target_name} "${source_file}")
  target_link_libraries(${target_name} velox_common Threads::Threads)
  list(APPEND example_targets ${target_name})
endforeach()

if (TARGET DynamicLoading)
  add_dependencies(DynamicLoading velox_dynamic_example)
  target_compile_definitions(
    DynamicLoading
    PRIVATE VELOX_DYNAMIC_EXAMPLE_LIB_PATH="$<TARGET_FILE:velox_dynamic_example>"
  )
  if (UNIX AND NOT APPLE)
    target_link_options(DynamicLoading PRIVATE -Wl,-export-dynamic)
  endif()
  if (APPLE)
    target_link_options(velox_dynamic_example PRIVATE -Wl,-undefined,dynamic_lookup)
    target_link_options(DynamicLoading PRIVATE -Wl,-export_dynamic)
  endif()
endif()

add_custom_target(run-examples
  DEPENDS ${example_targets}
)
foreach(target_name IN LISTS example_targets)
  add_custom_command(
    TARGET run-examples
    COMMAND $<TARGET_FILE:${target_name}>
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  )
endforeach()
