file(GLOB example_sources
  "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)
list(SORT example_sources)
set(example_targets)

foreach(source_file IN LISTS example_sources)
  get_filename_component(target_name "${source_file}" NAME_WE)
  add_executable(${target_name} "${source_file}")
  target_link_libraries(${target_name} velox_common Threads::Threads)
  list(APPEND example_targets ${target_name})
endforeach()

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
