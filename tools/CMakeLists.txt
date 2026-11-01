set(CINDER_TOOL_SOURCES cinderd_main.cpp cinder_cli.cpp cinder_admin.cpp)

add_executable(cinderd cinderd_main.cpp)
target_link_libraries(cinderd PRIVATE cinder::core CLI11::Module)
if(CINDER_ENABLE_GRPC)
    target_link_libraries(cinderd PRIVATE cinder_grpc)
endif()

add_executable(cinder-cli cinder_cli.cpp)
target_link_libraries(cinder-cli PRIVATE cinder::core CLI11::Module)

add_executable(cinder-admin cinder_admin.cpp)
target_link_libraries(cinder-admin PRIVATE cinder::core CLI11::Module)
# The project disables C++ scanning globally (CMAKE_CXX_SCAN_FOR_MODULES
# OFF); module consumers must opt back in so CMake wires the BMI deps,
# the module map, and -fmodules-ts for GCC.
# import std needs a per-target opt-in (never global: it would also tag
# FetchContent deps) and C++23 (already the project default). Support is
# implied by the top-level compiler floors (GCC >= 16, Clang >= 22 need
# GCC >= 15 / Clang >= 18.1.2 for import std); do NOT probe
# CMAKE_CXX_COMPILER_IMPORT_STD here — it is only populated during compiler
# detection, so it reads empty on reconfigured (cached-compiler) build dirs
# and would false-positive the gate.
set_target_properties(cinderd cinder-cli cinder-admin PROPERTIES CXX_SCAN_FOR_MODULES ON)
set_target_properties(cinderd cinder-cli cinder-admin PROPERTIES CXX_MODULE_STD 1)

set_target_properties(cinderd cinder-cli cinder-admin PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
_cinder_apply_debug_options(cinderd)
_cinder_apply_debug_options(cinder-cli)
_cinder_apply_debug_options(cinder-admin)
cinder_reuse_pch(cinderd)
cinder_reuse_pch(cinder-cli)
cinder_reuse_pch(cinder-admin)

list(TRANSFORM CINDER_TOOL_SOURCES PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} PREFIX tools FILES ${CINDER_TOOL_SOURCES})
