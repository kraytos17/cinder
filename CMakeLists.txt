cmake_minimum_required(VERSION 4.4)

project(cinder VERSION 0.1.0
        DESCRIPTION
        "Cinder — a distributed, consistent-hash-sharded in-memory cache"
        LANGUAGES CXX)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "16")
    message(FATAL_ERROR "Cinder requires GCC >= 16 (found ${CMAKE_CXX_COMPILER_VERSION})")
endif()
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "22")
    message(FATAL_ERROR "Cinder requires Clang >= 22 (found ${CMAKE_CXX_COMPILER_VERSION})")
endif()

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
option(CINDER_BUILD_BENCHMARKS      "Build throughput/latency benchmarks"    OFF)
option(CINDER_ENABLE_TLS            "Build with Asio SSL / OpenSSL support"  OFF)
option(CINDER_ENABLE_CRYPTO         "Build with OpenSSL crypto (HMAC-SHA256 for node auth)" OFF)
option(CINDER_ENABLE_ASAN           "Build with AddressSanitizer"            OFF)
option(CINDER_ENABLE_UBSAN          "Build with UndefinedBehaviorSanitizer"  OFF)
option(CINDER_ENABLE_TSAN           "Build with ThreadSanitizer"             OFF)
option(CINDER_ENABLE_MSAN           "Build with MemorySanitizer (Clang, needs instrumented C++ stdlib)" OFF)
option(CINDER_ENABLE_FUZZER         "Build with libFuzzer (implies ASan, Clang only)" OFF)
option(CINDER_ENABLE_COVERAGE       "Build with coverage instrumentation (Clang only)" OFF)
option(CINDER_ENABLE_LTO            "Enable link-time optimization"          OFF)
option(CINDER_UNITY_BUILD           "Enable unity/jumbo builds"              OFF)
option(CINDER_ENABLE_CLANG_TIDY     "Run clang-tidy during compilation"      OFF)
option(CINDER_ENABLE_IWYU           "Run include-what-you-use during compilation" OFF)
option(CINDER_ENABLE_GCC_ANALYZER   "Run GCC's static analyzer (-fanalyzer, GCC only)" OFF)
option(CINDER_USE_PIPE              "Use -pipe (pipes over temp files)"      ON)
option(CINDER_USE_FAST_LINKER       "Prefer mold, then lld, over the default" ON)
option(CINDER_USE_SPLIT_DWARF       "Use -gsplit-dwarf for faster linking"    ON)
option(CINDER_KEEP_FRAME_POINTER    "Keep frame pointer (-fno-omit-frame-pointer) for perf/profiling" OFF)
option(CINDER_TEST_BUILD_DEPS       "Auto-build test dependencies (Ninja)"   ON)
option(CINDER_USE_MIMALLOC          "Override global allocator with mimalloc" ON)
option(CINDER_ENABLE_GRPC          "Build with gRPC gateway for multi-language clients" OFF)
option(CINDER_PREFER_SYSTEM_DEPS    "Try find_package() before FetchContent for each dep" ON)

# Build-speed options -------------------------------------------------------
set(CINDER_CACHE_TOOL "AUTO" CACHE STRING
    "Compiler cache: AUTO (ccache, then sccache), CCACHE, SCCACHE or NONE")
set_property(CACHE CINDER_CACHE_TOOL PROPERTY STRINGS AUTO CCACHE SCCACHE NONE)
option(CINDER_USE_PCH               "Use precompiled headers for cinder_core (and targets calling cinder_reuse_pch)" ON)
option(CINDER_PCH_GCC_WITH_CCACHE   "Keep PCH on for GCC when ccache is active (needs sloppiness; LLVM disables it in that case — benchmark both)" ON)
option(CINDER_CCACHE_RELOCATABLE    "Make ccache hits independent of checkout path (hash_dir off + -ffile-prefix-map); useful for worktrees/CI" OFF)
option(CINDER_FAST_LINKER_WITH_GCC_LTO "Allow mold/lld together with GCC LTO (mold supports GCC LTO via the linker plugin; retest before enabling)" OFF)
option(CINDER_OPTIMIZE_DEPENDENCIES "Set CMAKE_OPTIMIZE_DEPENDENCIES (skip needless static-lib build-order deps; disabled with gRPC)" ON)
set(CINDER_LINK_JOBS "" CACHE STRING
    "Max parallel link jobs (Ninja only). Empty = unlimited. Set to 1-2 for LTO/sanitizer presets")
option(CINDER_USE_DEPS_CACHE        "Share pinned dependency sources across presets/compilers (see cmake/CinderDeps.cmake)" ON)
option(CINDER_DEPS_AUTOFETCH        "Populate the shared dependency cache on configure when an entry is missing" ON)
set(CINDER_DEPS_CACHE "" CACHE PATH
    "Shared dependency source cache (default: $CINDER_DEPS_CACHE or ~/.cache/cinder/deps)")

# ---------------------------------------------------------------------------
# Compiler cache + precompiled headers (see cmake/CinderCompilerCache.cmake)
# ---------------------------------------------------------------------------
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CinderCompilerCache.cmake)

# Fast linker selection (see cmake/CinderLinker.cmake).
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CinderLinker.cmake)

# ---------------------------------------------------------------------------
# Instrumentation: fuzzer, coverage, sanitizers (see cmake/CinderSanitizers.cmake)
# ---------------------------------------------------------------------------
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CinderSanitizers.cmake)

# ---------------------------------------------------------------------------
# Compiler-integrated static analysis
# ---------------------------------------------------------------------------
if(CINDER_ENABLE_GCC_ANALYZER)
    if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        message(WARNING "CINDER_ENABLE_GCC_ANALYZER has no effect outside GCC")
    else()
        add_compile_options(-fanalyzer)
        message(STATUS "Cinder: GCC -fanalyzer enabled")
    endif()
endif()

# ---------------------------------------------------------------------------
# Build tuning: LTO, PGO, compile/link knobs, debug helper
# (see cmake/CinderOptimization.cmake)
# ---------------------------------------------------------------------------
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CinderOptimization.cmake)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
find_package(Threads REQUIRED)
include(FetchContent)

# Dependency sources are cached ONCE per (dependency, pinned tag) outside the
# build trees (see cmake/CinderDeps.cmake) and shared by every preset, compiler
# and git worktree: no per-compiler or per-preset clone. Each dependency's
# BINARY_DIR below stays per-preset, so compiler/sanitizer flags never collide.
# Pins live in cmake/CinderDepVersions.cmake.
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CinderDeps.cmake)

# Everything is pinned to fixed tags/SHAs, so skip FetchContent's update check
# on the fallback (non-cached) path. Not FORCEd: pass
# -DFETCHCONTENT_UPDATES_DISCONNECTED=OFF to override.
set(FETCHCONTENT_UPDATES_DISCONNECTED ON CACHE BOOL "")

message(STATUS "Cinder: dependency source cache -> ${CINDER_DEPS_CACHE} (empty = auto)")

# Retry flaky git clones instead of failing configure outright
set(CMAKE_EP_GIT_CLONE_RETRY_COUNT 3)
set(CMAKE_EP_GIT_CLONE_RETRY_DELAY 5)

# Fetches every third-party dependency (see cmake/CinderDependencies.cmake).
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CinderDependencies.cmake)
cinder_fetch_all_dependencies()

# ---------------------------------------------------------------------------
# gRPC gateway (opt-in via CINDER_ENABLE_GRPC; see cmake/CinderGrpc.cmake)
# ---------------------------------------------------------------------------
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CinderGrpc.cmake)

# Developer lint targets + static analyzers (see cmake/CinderLint.cmake).
# Included here (not at the end) because CMAKE_CXX_CLANG_TIDY and
# CMAKE_CXX_INCLUDE_WHAT_YOU_USE only affect targets created afterwards.
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CinderLint.cmake)

# ---------------------------------------------------------------------------
# Core library (target defined in src/; alias kept here as the public name)
# ---------------------------------------------------------------------------
add_subdirectory(src)

add_library(cinder::core ALIAS cinder_core)

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

# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------
install(TARGETS cinder_core cinderd cinder-cli
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
)
install(DIRECTORY include/cinder DESTINATION include)

# ---------------------------------------------------------------------------
# Pre-commit hook auto-install (see cmake/CinderGitHooks.cmake)
# ---------------------------------------------------------------------------
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/CinderGitHooks.cmake)
