cmake_minimum_required(VERSION 4.4)

project(cinder VERSION 0.1.0
        DESCRIPTION
        "Cinder — a distributed, consistent-hash-sharded in-memory cache"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING
        "Build type: Debug, Release, RelWithDebInfo, MinSizeRel" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
        "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif()

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
option(CINDER_BUILD_TESTS           "Build unit/integration tests"           ON)
option(CINDER_BUILD_BENCHMARKS      "Build throughput/latency benchmarks"    ON)
option(CINDER_ENABLE_TLS            "Build with Asio SSL / OpenSSL support"  OFF)
option(CINDER_USE_MIMALLOC          "Link mimalloc as the global allocator"  ON)
option(CINDER_SANITIZE              "Build with ASan+UBSan enabled"          OFF)
option(CINDER_ENABLE_LTO            "Enable link-time optimization"          OFF)
option(CINDER_UNITY_BUILD           "Enable unity/jumbo builds"              OFF)
option(CINDER_ENABLE_CLANG_TIDY     "Run clang-tidy during compilation"      OFF)
option(CINDER_USE_FAST_LINKER       "Use mold/lld linker if available"       ON)

# ---------------------------------------------------------------------------
# ccache
# ---------------------------------------------------------------------------
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    message(STATUS "Cinder: using ccache")
endif()

# ---------------------------------------------------------------------------
# Fast linker (mold > lld)
# ---------------------------------------------------------------------------
if(CINDER_USE_FAST_LINKER)
    find_program(MOLD_LINKER mold)
    find_program(LLD_LINKER ld.lld)

    if(MOLD_LINKER)
        set(CMAKE_EXE_LINKER_FLAGS    "-fuse-ld=mold ${CMAKE_EXE_LINKER_FLAGS}")
        set(CMAKE_SHARED_LINKER_FLAGS "-fuse-ld=mold ${CMAKE_SHARED_LINKER_FLAGS}")
        message(STATUS "Cinder: using mold linker")
    elseif(LLD_LINKER)
        set(CMAKE_EXE_LINKER_FLAGS    "-fuse-ld=lld ${CMAKE_EXE_LINKER_FLAGS}")
        set(CMAKE_SHARED_LINKER_FLAGS "-fuse-ld=lld ${CMAKE_SHARED_LINKER_FLAGS}")
        message(STATUS "Cinder: using lld linker")
    endif()
endif()

# ---------------------------------------------------------------------------
# Sanitizers
# ---------------------------------------------------------------------------
if(CINDER_SANITIZE)
    message(STATUS "Cinder: sanitizers enabled (ASan + UBSan)")
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address,undefined)
endif()

# ---------------------------------------------------------------------------
# LTO
# ---------------------------------------------------------------------------
if(CINDER_ENABLE_LTO)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
    message(STATUS "Cinder: LTO enabled")
endif()

# ---------------------------------------------------------------------------
# clang-tidy
# ---------------------------------------------------------------------------
if(CINDER_ENABLE_CLANG_TIDY)
    find_program(CLANG_TIDY_PROGRAM clang-tidy)
    if(CLANG_TIDY_PROGRAM)
        set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PROGRAM}")
        message(STATUS "Cinder: clang-tidy enabled (${CLANG_TIDY_PROGRAM})")
    else()
        message(WARNING "CINDER_ENABLE_CLANG_TIDY is ON but clang-tidy not found")
    endif()
endif()

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
find_package(Threads REQUIRED)
include(FetchContent)

FetchContent_Declare(
    asio
    GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
    GIT_TAG        asio-1-38-2
)
FetchContent_MakeAvailable(asio)
add_library(asio INTERFACE)
target_include_directories(asio INTERFACE ${asio_SOURCE_DIR}/asio/include)
target_compile_definitions(asio INTERFACE
    ASIO_STANDALONE
    ASIO_NO_DEPRECATED
)

FetchContent_Declare(
    xxhash
    GIT_REPOSITORY https://github.com/Cyan4973/xxHash.git
    GIT_TAG        v0.8.3
    SOURCE_SUBDIR  cmake_unofficial
)
FetchContent_MakeAvailable(xxhash)

FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.17.0
)
FetchContent_MakeAvailable(spdlog)

FetchContent_Declare(
    CLI11
    GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
    GIT_TAG        v2.6.2
)
FetchContent_MakeAvailable(CLI11)

if(CINDER_USE_MIMALLOC)
    FetchContent_Declare(
        mimalloc
        GIT_REPOSITORY https://github.com/microsoft/mimalloc.git
        GIT_TAG        v3.4.3
    )
    set(MI_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(MI_BUILD_TESTS   OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(mimalloc)
endif()

if(CINDER_ENABLE_TLS)
    find_package(OpenSSL REQUIRED)
    message(STATUS "Cinder: TLS enabled, using OpenSSL ${OPENSSL_VERSION}")
endif()

if(CINDER_BUILD_TESTS)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.17.0
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
endif()

if(CINDER_BUILD_BENCHMARKS)
    FetchContent_Declare(
        benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG        v1.9.5
    )
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(benchmark)
endif()

# ---------------------------------------------------------------------------
# Unity builds (only cinder targets, not fetched deps)
# ---------------------------------------------------------------------------
if(CINDER_UNITY_BUILD)
    set(CMAKE_UNITY_BUILD ON)
    message(STATUS "Cinder: unity builds enabled (cinder targets only)")
endif()

# ---------------------------------------------------------------------------
# Core library
# ---------------------------------------------------------------------------
add_subdirectory(src)

add_library(cinder::core ALIAS cinder_core)

target_include_directories(cinder_core PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(cinder_core PUBLIC
    Threads::Threads
    asio
    xxhash
    spdlog::spdlog
)

if(CINDER_ENABLE_TLS)
    target_link_libraries(cinder_core PUBLIC OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(cinder_core PUBLIC CINDER_ENABLE_TLS)
endif()

if(CINDER_USE_MIMALLOC)
    target_link_libraries(cinder_core PUBLIC mimalloc-static)
endif()

target_compile_options(cinder_core PRIVATE
    -Wall -Wextra -Wpedantic
    $<$<CONFIG:Release>:-O3>
)

target_precompile_headers(cinder_core PRIVATE
    <asio.hpp>
    <spdlog/spdlog.h>
    <xxhash.h>
)

# ---------------------------------------------------------------------------
# Executables
# ---------------------------------------------------------------------------
add_subdirectory(tools)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(CINDER_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------
if(CINDER_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

# ---------------------------------------------------------------------------
# Format / lint targets
# ---------------------------------------------------------------------------
file(GLOB_RECURSE CINDER_FORMAT_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cinder/*.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tools/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/*.cpp
)

find_program(CLANG_FORMAT_PROGRAM clang-format)
if(CLANG_FORMAT_PROGRAM)
    add_custom_target(format
        COMMAND ${CLANG_FORMAT_PROGRAM} -i -style=file ${CINDER_FORMAT_SOURCES}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Formatting all source files with clang-format"
    )
    add_custom_target(check-format
        COMMAND ${CLANG_FORMAT_PROGRAM} --dry-run --Werror -style=file ${CINDER_FORMAT_SOURCES}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Checking formatting with clang-format"
    )
endif()

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------
install(TARGETS cinder_core cinderd cinder-cli
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
)
install(DIRECTORY include/cinder DESTINATION include)
