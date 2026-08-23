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
option(CINDER_ENABLE_ASAN           "Build with AddressSanitizer"            OFF)
option(CINDER_ENABLE_UBSAN          "Build with UndefinedBehaviorSanitizer"  OFF)
option(CINDER_ENABLE_TSAN           "Build with ThreadSanitizer"             OFF)
option(CINDER_ENABLE_MSAN           "Build with MemorySanitizer (Clang, needs instrumented libc++)" OFF)
option(CINDER_ENABLE_LTO            "Enable link-time optimization"          OFF)
option(CINDER_UNITY_BUILD           "Enable unity/jumbo builds"              OFF)
option(CINDER_ENABLE_CLANG_TIDY     "Run clang-tidy during compilation"      OFF)
option(CINDER_USE_PIPE              "Use -pipe (pipes over temp files)"      ON)
option(CINDER_USE_FAST_LINKER       "Prefer mold, then lld, over the default" ON)
option(CINDER_USE_SPLIT_DWARF       "Use -gsplit-dwarf for faster linking"    ON)
option(CINDER_TEST_BUILD_DEPS       "Auto-build test dependencies (Ninja)"   ON)
option(CINDER_USE_MIMALLOC          "Override global allocator with mimalloc" ON)
option(CINDER_PREFER_SYSTEM_DEPS    "Try find_package() before FetchContent for each dep" ON)

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

# ---------------------------------------------------------------------------
# Fast linker (mold > lld > default)
# ---------------------------------------------------------------------------
if(CINDER_USE_FAST_LINKER)
    find_program(MOLD_PROGRAM mold)
    if(MOLD_PROGRAM)
        set(CMAKE_LINKER_TYPE MOLD)
        message(STATUS "Cinder: using mold linker")
    else()
        find_program(LLD_PROGRAM ld.lld)
        if(LLD_PROGRAM)
            set(CMAKE_LINKER_TYPE LLD)
            message(STATUS "Cinder: using lld linker")
        else()
            message(STATUS "Cinder: no fast linker found, using default")
        endif()
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
if(CINDER_USE_PIPE)
    add_compile_options(-pipe)
endif()

# Split DWARF: emit per-TU debug info into .dwo files instead of one giant
# .debug_info section in the executable. Faster linking + less disk I/O.
# -Wl,--gdb-index pre-builds the index so gdb doesn't scan .dwo at load —
# requires a fast linker (mold/lld) that supports it.
if(CINDER_USE_SPLIT_DWARF AND CMAKE_BUILD_TYPE MATCHES "Debug|RelWithDebInfo")
    add_compile_options(-gsplit-dwarf)
    if(CINDER_USE_FAST_LINKER AND CMAKE_LINKER_TYPE MATCHES "MOLD|LLD")
        add_link_options(-Wl,--gdb-index)
    endif()
    message(STATUS "Cinder: split-dwarf enabled")
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

# Share fetchcontent source downloads across all presets of the SAME
# compiler family, avoiding N clones per new build/asan/tsan/etc directory.
# Partitioned by compiler ID (clang vs gcc) rather than a single shared
# dir: FETCHCONTENT_BASE_DIR governs the default -src, -subbuild, AND
# -build locations for every dep, and mixing object files/build trees
# produced by two different compilers/ABIs in one shared build dir is
# unsafe. Each compiler family gets its own source+subbuild cache under
# .cache/fetchcontent-<compiler>/; each preset still gets its own
# per-target BINARY_DIR under binaryDir/_deps/<name>-build below so
# compiler/sanitizer flags never collide on the compiled artifacts.
# .cache/ is already gitignored.
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(_cinder_toolchain_key "clang")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(_cinder_toolchain_key "gcc")
else()
    string(TOLOWER "${CMAKE_CXX_COMPILER_ID}" _cinder_toolchain_key)
endif()

set(FETCHCONTENT_BASE_DIR
    "${CMAKE_SOURCE_DIR}/.cache/fetchcontent-${_cinder_toolchain_key}"
    CACHE PATH "Shared FetchContent source/subbuild cache, partitioned per compiler" FORCE)

# All our deps are pinned to fixed tags, never floating branches, so there
# is nothing for FetchContent to "update" to on reconfigure — skip the
# git-fetch existence/update check it otherwise runs every configure.
set(FETCHCONTENT_UPDATES_DISCONNECTED ON CACHE BOOL "" FORCE)

message(STATUS "Cinder: FetchContent cache -> ${FETCHCONTENT_BASE_DIR}")

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

# Common FIND_PACKAGE_ARGS toggle: when ON, each FetchContent_Declare below
# tries find_package() first (system/vcpkg/conan install) and only falls
# back to cloning + building from source if nothing satisfying is found.
# Skips the clang/gcc duplication entirely for anything already installed.
set(_cinder_fp_args "")
if(CINDER_PREFER_SYSTEM_DEPS)
    set(_cinder_fp_args FIND_PACKAGE_ARGS)
endif()

FetchContent_Declare(
    asio
    GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
    GIT_TAG        asio-1-38-2
    GIT_SHALLOW    TRUE
    GIT_SUBMODULES ""
    BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/asio-build
    SYSTEM
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
    GIT_SUBMODULES ""
    BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/xxhash-build
    SYSTEM
)
FetchContent_MakeAvailable(xxhash)

FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.17.0
    GIT_SHALLOW    TRUE
    GIT_SUBMODULES ""
    BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/spdlog-build
    SYSTEM
    ${_cinder_fp_args} NAMES spdlog
)
set(SPDLOG_BUILD_SHARED OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(spdlog)

FetchContent_Declare(
    CLI11
    GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
    GIT_TAG        v2.7.2
    GIT_SHALLOW    TRUE
    GIT_SUBMODULES ""
    BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/cli11-build
    SYSTEM
    ${_cinder_fp_args} NAMES CLI11
)
FetchContent_MakeAvailable(CLI11)

FetchContent_Declare(
    yaml-cpp
    GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
    GIT_TAG        yaml-cpp-0.9.0
    GIT_SHALLOW    TRUE
    GIT_SUBMODULES ""
    BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/yaml-cpp-build
    SYSTEM
    ${_cinder_fp_args} NAMES yaml-cpp
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)

# mimalloc — global allocator override. Disabled automatically under any
# sanitizer build: ASan/TSan/MSan each install their own allocation hooks,
# and layering mimalloc's override underneath produces false positives /
# crashes rather than useful signal. Sanitizer builds use the sanitizer's
# own allocator instead, same mutual-exclusion spirit as the ASan/TSan/MSan
# checks above.
#
# No FIND_PACKAGE_ARGS here on purpose: we need the exact v3.5.0 build with
# MI_OVERRIDE/MI_BUILD_STATIC set our way, not whatever a system package
# happens to ship — a mismatched system mimalloc build would silently defeat
# the point of this dep.
if(CINDER_USE_MIMALLOC AND (CINDER_ENABLE_ASAN OR CINDER_ENABLE_TSAN OR CINDER_ENABLE_MSAN))
    message(STATUS "Cinder: mimalloc disabled — sanitizer build takes precedence")
    set(CINDER_USE_MIMALLOC OFF)
endif()

if(CINDER_USE_MIMALLOC)
    FetchContent_Declare(
        mimalloc
        GIT_REPOSITORY https://github.com/microsoft/mimalloc.git
        GIT_TAG        v3.5.0
        GIT_SHALLOW    TRUE
        GIT_SUBMODULES ""
        BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/mimalloc-build
        SYSTEM
    )

    set(MI_BUILD_TESTS  OFF CACHE BOOL "" FORCE)
    set(MI_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(MI_BUILD_OBJECT OFF CACHE BOOL "" FORCE)
    set(MI_BUILD_STATIC ON  CACHE BOOL "" FORCE)
    set(MI_OVERRIDE     ON  CACHE BOOL "" FORCE)   # provides malloc/free overrides
    FetchContent_MakeAvailable(mimalloc)
    message(STATUS "Cinder: mimalloc enabled (static override)")
endif()

if(CINDER_ENABLE_TLS)
    find_package(OpenSSL REQUIRED)
    message(STATUS "Cinder: TLS enabled, using OpenSSL ${OPENSSL_VERSION}")
endif()

if(CINDER_BUILD_TESTS)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.18.0
        GIT_SHALLOW    TRUE
        GIT_SUBMODULES ""
        BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/googletest-build
        SYSTEM
        EXCLUDE_FROM_ALL
        ${_cinder_fp_args} NAMES GTest
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
        GIT_SUBMODULES ""
        BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/benchmark-build
        SYSTEM
        EXCLUDE_FROM_ALL
        ${_cinder_fp_args} NAMES benchmark
    )
    set(BENCHMARK_ENABLE_TESTING        OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL        OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_WERROR         OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_GTEST_TESTS    OFF CACHE BOOL "" FORCE)
    # Without this, Google Benchmark FetchContents its OWN separate copy of
    # googletest for its self-tests, even though we already fetch one above
    # when CINDER_BUILD_TESTS is on — two independent googletest checkouts
    # for no reason.
    set(BENCHMARK_DOWNLOAD_DEPENDENCIES OFF CACHE BOOL "" FORCE)
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
    yaml-cpp::yaml-cpp
)

# mimalloc must come before other libs on the link line so its malloc/free
# symbols are the ones the linker resolves against. PUBLIC so every
# executable linking cinder::core (cinderd, cinder-cli, tests, benchmarks)
# picks up the override automatically.
if(CINDER_USE_MIMALLOC)
    target_link_libraries(cinder_core PUBLIC mimalloc-static)
    target_compile_definitions(cinder_core PUBLIC CINDER_USE_MIMALLOC)
endif()

if(CINDER_ENABLE_TLS)
    target_link_libraries(cinder_core PUBLIC OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(cinder_core PUBLIC CINDER_ENABLE_TLS)
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
