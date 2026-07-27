add_executable(cinderd cinderd_main.cpp)
target_link_libraries(cinderd PRIVATE cinder::core CLI11::CLI11)

add_executable(cinder-cli cinder_cli.cpp)
target_link_libraries(cinder-cli PRIVATE cinder::core CLI11::CLI11)

set_target_properties(cinderd cinder-cli PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)
