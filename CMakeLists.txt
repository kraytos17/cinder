cmake_minimum_required(VERSION 4.4)

project(cinder VERSION 0.1.0
        DESCRIPTION
        "Cinder — a distributed, consistent-hash-sharded in-memory cache"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

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
option(CINDER_ENABLE_ASAN           "Build with AddressSanitizer"            OFF)
option(CINDER_ENABLE_UBSAN          "Build with UndefinedBehaviorSanitizer"  OFF)
option(CINDER_ENABLE_TSAN           "Build with ThreadSanitizer"             OFF)
option(CINDER_ENABLE_MSAN           "Build with MemorySanitizer (Clang, needs instrumented libc++)" OFF)
option(CINDER_ENABLE_LTO            "Enable link-time optimization"          OFF)
option(CINDER_UNITY_BUILD           "Enable unity/jumbo builds"              OFF)
option(CINDER_ENABLE_CLANG_TIDY     "Run clang-tidy during compilation"      OFF)
option(CINDER_USE_FAST_LINKER       "Use mold/lld linker if available"       ON)
option(CINDER_USE_SPLIT_DWARF       "Use -gsplit-dwarf for faster linking"   ON)
option(CINDER_USE_PIPE              "Use -pipe (pipes over temp files)"      ON)
option(CINDER_TEST_BUILD_DEPS       "Auto-build test dependencies (Ninja)"   ON)

# ---------------------------------------------------------------------------
# ccache
# ---------------------------------------------------------------------------
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    set(CCACHE_BASEDIR "${CMAKE_SOURCE_DIR}")
    message(STATUS "Cinder: using ccache (basedir: ${CCACHE_BASEDIR})")
endif()

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
if(CINDER_ENABLE_ASAN AND CINDER_ENABLE_TSAN)
    message(FATAL_ERROR "ASan and TSAN are mutually exclusive — enable only one")
endif()

if(CINDER_ENABLE_MSAN AND (CINDER_ENABLE_ASAN OR CINDER_ENABLE_TSAN))
    message(FATAL_ERROR "MSan, ASan, and TSan each use conflicting shadow-memory layouts — mutual exclusion required")
endif()

if(CINDER_ENABLE_ASAN OR CINDER_ENABLE_TSAN OR CINDER_ENABLE_MSAN)
    if(CINDER_USE_MIMALLOC)
        message(WARNING "mimalloc conflicts with ASan/TSAN/MSan/LSan — forcing CINDER_USE_MIMALLOC=OFF")
        set(CINDER_USE_MIMALLOC OFF)
    endif()
endif()

set(SANITIZER_FLAGS "")
set(SANITIZER_LINK_FLAGS "")
if(CINDER_ENABLE_ASAN)
    list(APPEND SANITIZER_FLAGS "-fsanitize=address")
    list(APPEND SANITIZER_LINK_FLAGS "-fsanitize=address")
    message(STATUS "Cinder: AddressSanitizer enabled")
endif()

if(CINDER_ENABLE_UBSAN)
    list(APPEND SANITIZER_FLAGS "-fsanitize=undefined")
    list(APPEND SANITIZER_LINK_FLAGS "-fsanitize=undefined")
    message(STATUS "Cinder: UndefinedBehaviorSanitizer enabled")
endif()

if(CINDER_ENABLE_TSAN)
    list(APPEND SANITIZER_FLAGS "-fsanitize=thread")
    list(APPEND SANITIZER_LINK_FLAGS "-fsanitize=thread")
    message(STATUS "Cinder: ThreadSanitizer enabled")
endif()

if(CINDER_ENABLE_MSAN)
    list(APPEND SANITIZER_FLAGS "-fsanitize=memory")
    list(APPEND SANITIZER_LINK_FLAGS "-fsanitize=memory")
    message(STATUS "Cinder: MemorySanitizer enabled")
    message(WARNING "MSan requires ALL code (including libc++) to be MSan-instrumented. "
            "Expected false positives from uninstrumented system libraries.")
endif()

if(SANITIZER_FLAGS)
    add_compile_options(${SANITIZER_FLAGS} -fno-omit-frame-pointer -g)
    add_link_options(${SANITIZER_LINK_FLAGS})
    # Clang-specific ASan extras: use-after-scope, use-after-return
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND CINDER_ENABLE_ASAN)
        add_compile_options(
            -fsanitize-address-use-after-scope
            -fsanitize-address-use-after-return=always
        )
        message(STATUS "Cinder: ASan extras enabled (use-after-scope, use-after-return)")
    endif()
endif()

# ---------------------------------------------------------------------------
# LTO
# ---------------------------------------------------------------------------
if(CINDER_ENABLE_LTO)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
    message(STATUS "Cinder: LTO enabled")
endif()

# ---------------------------------------------------------------------------
# Compile-time optimizations
# ---------------------------------------------------------------------------
if(CINDER_USE_SPLIT_DWARF AND CMAKE_BUILD_TYPE MATCHES "Debug|RelWithDebInfo")
    add_compile_options(-gsplit-dwarf)
    if(CINDER_USE_FAST_LINKER)
        add_link_options(-Wl,--gdb-index)
    endif()
    message(STATUS "Cinder: split-dwarf enabled")
endif()

if(CINDER_USE_PIPE)
    add_compile_options(-pipe)
endif()

if(CINDER_UNITY_BUILD)
    set(CMAKE_UNITY_BUILD ON)
    set(CMAKE_UNITY_BUILD_BATCH_SIZE 16 CACHE STRING
        "Number of TUs merged per unity batch")
    message(STATUS "Cinder: unity builds enabled (cinder targets only)")
endif()

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
find_package(Threads REQUIRED)
include(FetchContent)

# Share fetchcontent downloads across all presets — avoids 7 clones per
# new build/asan/tsan/etc directory. Sources are cached in .cache/ (already
# gitignored); only the compiled objects are per-preset.
set(FETCHCONTENT_BASE_DIR "${CMAKE_SOURCE_DIR}/.cache/fetchcontent")

# Retry flaky git clones instead of failing configure outright
set(CMAKE_EP_GIT_CLONE_RETRY_COUNT 3)
set(CMAKE_EP_GIT_CLONE_RETRY_DELAY 5)

# Compile-time ignorelist for sanitizers — global so it covers third-party
# deps built via FetchContent (xxHash globals, etc.)
# Clang-only; GCC does not support -fsanitize-ignorelist.
set(CINDER_SANITIZER_IGNORELIST "${CMAKE_SOURCE_DIR}/suppressions/asan_ignorelist.txt")
if(EXISTS "${CINDER_SANITIZER_IGNORELIST}" AND CINDER_ENABLE_ASAN AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_compile_options(-fsanitize-ignorelist=${CINDER_SANITIZER_IGNORELIST})
    message(STATUS "Cinder: using sanitizer ignorelist")
endif()

FetchContent_Declare(
    asio
    GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
    GIT_TAG        asio-1-38-2
    GIT_SHALLOW    TRUE
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
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(xxhash)

FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.17.0
    GIT_SHALLOW    TRUE
)
set(SPDLOG_BUILD_SHARED OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(spdlog)

FetchContent_Declare(
    CLI11
    GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
    GIT_TAG        v2.6.2
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(CLI11)

if(CINDER_USE_MIMALLOC)
    FetchContent_Declare(
        mimalloc
        GIT_REPOSITORY https://github.com/microsoft/mimalloc.git
        GIT_TAG        v3.4.3
        GIT_SHALLOW    TRUE
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
        GIT_SHALLOW    TRUE
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
endif()

if(CINDER_BUILD_BENCHMARKS)
    FetchContent_Declare(
        benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG        v1.9.5
        GIT_SHALLOW    TRUE
    )
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(benchmark)
endif()

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
if(CINDER_TEST_BUILD_DEPS AND CMAKE_GENERATOR MATCHES "Ninja")
    set(CMAKE_TEST_BUILD_DEPENDS ON)
endif()

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
    add_custom_target(check-format COMMAND ${CLANG_FORMAT_PROGRAM} --dry-run
                      --Werror -style=file ${CINDER_FORMAT_SOURCES}
                      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} COMMENT
                      "Checking formatting with clang-format")
endif()

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------
install(TARGETS cinder_core cinderd cinder-cli
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
)
install(DIRECTORY include/cinder DESTINATION include)
