set(CINDER_TOOL_SOURCES cinderd_main.cpp cinder_cli.cpp)

add_executable(cinderd cinderd_main.cpp)
target_link_libraries(cinderd PRIVATE cinder::core CLI11::CLI11)

add_executable(cinder-cli cinder_cli.cpp)
target_link_libraries(cinder-cli PRIVATE cinder::core CLI11::CLI11)

set_target_properties(cinderd cinder-cli PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
    UNITY_BUILD ON
)

list(TRANSFORM CINDER_TOOL_SOURCES PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} PREFIX tools FILES ${CINDER_TOOL_SOURCES})
