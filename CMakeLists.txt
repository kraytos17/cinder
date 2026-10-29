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
# Compiler cache
# ---------------------------------------------------------------------------
# ccache is preferred over sccache: sccache cannot handle GCC precompiled
# headers (cinder_core uses a PCH), ccache can once it is configured for PCH.
# The ccache settings are passed through the launcher (cmake -E env) rather than
# the user's ccache.conf so they apply identically in every preset, under
# `just`, in CI and in IDEs. NB: a CMake variable named CCACHE_BASEDIR does
# nothing — ccache only reads the environment variable / config key.
set(_cinder_cache_kind "")
if(CINDER_CACHE_TOOL STREQUAL "AUTO" OR CINDER_CACHE_TOOL STREQUAL "CCACHE")
    find_program(CCACHE_PROGRAM ccache)
    if(CCACHE_PROGRAM)
        set(_cinder_cache_kind ccache)
    endif()
endif()
if(NOT _cinder_cache_kind AND (CINDER_CACHE_TOOL STREQUAL "AUTO" OR CINDER_CACHE_TOOL STREQUAL "SCCACHE"))
    find_program(SCCACHE_PROGRAM sccache)
    if(SCCACHE_PROGRAM)
        set(_cinder_cache_kind sccache)
    endif()
endif()

set(_cinder_use_pch ${CINDER_USE_PCH})
if(_cinder_cache_kind STREQUAL "ccache")
    set(_cinder_ccache_env
        CCACHE_BASEDIR=${CMAKE_SOURCE_DIR}
        # Required for precompiled headers (ccache manual, "Precompiled headers").
        CCACHE_SLOPPINESS=pch_defines,time_macros
        # Toolchains (toolchains/gcc16.cmake, clang22.cmake) may be rebuilt in
        # place; hash the compiler binary instead of trusting its mtime.
        CCACHE_COMPILERCHECK=content)
    if(CINDER_CCACHE_RELOCATABLE)
        list(APPEND _cinder_ccache_env CCACHE_NOHASHDIR=1)
        add_compile_options(-ffile-prefix-map=${CMAKE_SOURCE_DIR}=.)
    endif()
    set(_cinder_launcher ${CMAKE_COMMAND} -E env ${_cinder_ccache_env} ${CCACHE_PROGRAM})
    set(CMAKE_CXX_COMPILER_LAUNCHER "${_cinder_launcher}")
    set(CMAKE_C_COMPILER_LAUNCHER   "${_cinder_launcher}")
    message(STATUS "Cinder: using ccache (basedir: ${CMAKE_SOURCE_DIR})")
elseif(_cinder_cache_kind STREQUAL "sccache")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${SCCACHE_PROGRAM}")
    set(CMAKE_C_COMPILER_LAUNCHER   "${SCCACHE_PROGRAM}")
    message(STATUS "Cinder: using sccache")
    if(_cinder_use_pch AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        message(STATUS "Cinder: sccache cannot cache GCC PCH builds -> precompiled headers disabled "
                       "(install ccache, or use -DCINDER_CACHE_TOOL=CCACHE)")
        set(_cinder_use_pch OFF)
    endif()
endif()

if(_cinder_use_pch AND _cinder_cache_kind STREQUAL "ccache"
   AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND NOT CINDER_PCH_GCC_WITH_CCACHE)
    message(STATUS "Cinder: GCC + ccache -> precompiled headers disabled (CINDER_PCH_GCC_WITH_CCACHE=OFF)")
    set(_cinder_use_pch OFF)
endif()

if(NOT _cinder_use_pch)
    set(CMAKE_DISABLE_PRECOMPILE_HEADERS ON)
elseif(_cinder_cache_kind STREQUAL "ccache")
    # Flags ccache needs to cache compilations that use a PCH.
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:SHELL:-Xclang -fno-pch-timestamp>")
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-fpch-preprocess>")
    endif()
endif()

# NB: GCC LTO emits slim bytecode objects that need the linker plugin. This
# project originally saw `undefined symbol: main` when combining them with
# mold/lld, so the combination is gated out by default for exactly GNU+LTO.
# Current mold supports GCC LTO through the standard linker-plugin interface
# (lld does not), so retest with -DCINDER_FAST_LINKER_WITH_GCC_LTO=ON and
# flip the default if it links. Clang LTO (LLVM bitcode) works with both.
if(CINDER_USE_FAST_LINKER AND (CINDER_FAST_LINKER_WITH_GCC_LTO
                               OR NOT (CINDER_ENABLE_LTO AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")))
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
# Fuzzing (libFuzzer, Clang only) — must resolve before the sanitizer flags
# are built below, since a fuzzer build implies ASan.
# ---------------------------------------------------------------------------
if(CINDER_ENABLE_FUZZER)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "CINDER_ENABLE_FUZZER requires Clang (libFuzzer is a Clang/LLVM feature)")
    endif()
    if(CINDER_ENABLE_TSAN OR CINDER_ENABLE_MSAN)
        message(FATAL_ERROR "Fuzzer builds conflict with TSan/MSan — enable only one instrumentation mode")
    endif()
    set(CINDER_ENABLE_ASAN ON CACHE BOOL "" FORCE)
    message(STATUS "Cinder: libFuzzer enabled (Clang, implies ASan)")
endif()

# ---------------------------------------------------------------------------
# Coverage instrumentation (Clang only) — for corpus minimization
# ---------------------------------------------------------------------------
if(CINDER_ENABLE_COVERAGE)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "CINDER_ENABLE_COVERAGE requires Clang (llvm-cov/llvm-profdata)")
    endif()
    set(CINDER_ENABLE_ASAN ON CACHE BOOL "" FORCE)
    message(STATUS "Cinder: coverage instrumentation enabled (Clang, implies ASan)")
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

if(CINDER_ENABLE_FUZZER)
    # -fsanitize=fuzzer is applied ONLY to fuzz test targets in tests/CMakeLists.txt,
    # not globally, to avoid duplicate main() in non-fuzz targets like cinderd.
    message(STATUS "Cinder: libFuzzer enabled (Clang, implies ASan)")
endif()

if(CINDER_ENABLE_UBSAN)
    list(APPEND SANITIZER_FLAGS "-fsanitize=undefined")
    list(APPEND SANITIZER_LINK_FLAGS "-fsanitize=undefined")
    message(STATUS "Cinder: UndefinedBehaviorSanitizer enabled")
endif()

if(CINDER_ENABLE_TSAN)
    list(APPEND SANITIZER_FLAGS "-fsanitize=thread")
    list(APPEND SANITIZER_LINK_FLAGS "-fsanitize=thread")
    # TSan needs a fixed memory layout: PIE binaries require ASLR disabled
    # via personality(ADDR_NO_RANDOMIZE), which Docker's default seccomp
    # profile blocks — every TSan binary (including gtest discovery runs)
    # dies at startup in containers. Non-PIE executables load at a fixed
    # address, so ASLR can stay on. Exe-only: shared libs are unaffected.
    string(APPEND CMAKE_EXE_LINKER_FLAGS " -no-pie")
    message(STATUS "Cinder: ThreadSanitizer enabled")
endif()

if(CINDER_ENABLE_MSAN)
    list(APPEND SANITIZER_FLAGS "-fsanitize=memory")
    list(APPEND SANITIZER_LINK_FLAGS "-fsanitize=memory")
    message(STATUS "Cinder: MemorySanitizer enabled")
    message(WARNING "MSan requires ALL code (including the C++ standard library) to be MSan-instrumented. "
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

if(CINDER_KEEP_FRAME_POINTER AND NOT SANITIZER_FLAGS)
    add_compile_options(-fno-omit-frame-pointer)
    message(STATUS "Cinder: frame pointer preserved for profiling")
endif()

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
# LTO
# ---------------------------------------------------------------------------
if(CINDER_ENABLE_LTO)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
    message(STATUS "Cinder: LTO enabled")
    # lld's ThinLTO cache makes LTO relinks incremental (Clang only).
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND CMAKE_LINKER_TYPE STREQUAL "LLD")
        add_link_options("LINKER:--thinlto-cache-dir=${CMAKE_BINARY_DIR}/thinlto-cache")
        message(STATUS "Cinder: ThinLTO cache -> ${CMAKE_BINARY_DIR}/thinlto-cache")
    endif()
endif()

# ---------------------------------------------------------------------------
# Compile-time optimizations
# ---------------------------------------------------------------------------
if(CINDER_USE_PIPE)
    add_compile_options(-pipe)
endif()

# Ninja: cap concurrent links (LTO / sanitizer links are memory-hungry), and
# drop static-library build-order edges that are not actually needed.
if(CINDER_LINK_JOBS AND CMAKE_GENERATOR MATCHES "Ninja")
    set_property(GLOBAL APPEND PROPERTY JOB_POOLS cinder_link=${CINDER_LINK_JOBS})
    set(CMAKE_JOB_POOL_LINK cinder_link)
    message(STATUS "Cinder: Ninja link pool limited to ${CINDER_LINK_JOBS} job(s)")
endif()
if(CINDER_OPTIMIZE_DEPENDENCIES AND NOT CINDER_ENABLE_GRPC)
    set(CMAKE_OPTIMIZE_DEPENDENCIES ON)
endif()

# ---------------------------------------------------------------------------
# Debug-flag helper — applies -Og/-O1, -gdwarf-5, -fdebug-types-section, and
# -gsplit-dwarf only to our own targets, never to FetchContent'd dependencies.
# Call this on every cinder target after it is created.
# ---------------------------------------------------------------------------
function(_cinder_apply_debug_options target)
    target_compile_options(${target} PRIVATE
        $<$<AND:$<CONFIG:Debug>,$<CXX_COMPILER_ID:GNU>>:-Og>
        $<$<AND:$<CONFIG:Debug>,$<CXX_COMPILER_ID:Clang>>:-O1>
    )
    if(CMAKE_BUILD_TYPE MATCHES "Debug|RelWithDebInfo")
        target_compile_options(${target} PRIVATE -gdwarf-5 -fdebug-types-section)
    endif()
    # Clang hard-errors when a shared PCH is combined with -gsplit-dwarf: the
    # .dwo path is baked into the PCH at the point the PCH itself is compiled,
    # and it never matches the .dwo path of whichever TU later includes that
    # PCH ("definition of macro '_gsplit_dwarf_path' differs..."). GCC only
    # warns and silently discards the PCH for the mismatching TU, so split
    # DWARF is safe there. Restrict split-dwarf to non-Clang compilers.
    if(CINDER_USE_SPLIT_DWARF AND CMAKE_BUILD_TYPE MATCHES "Debug|RelWithDebInfo" AND NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        target_compile_options(${target} PRIVATE -gsplit-dwarf)
        if(CINDER_USE_FAST_LINKER AND CMAKE_LINKER_TYPE MATCHES "MOLD|LLD")
            target_link_options(${target} PRIVATE -Wl,--gdb-index)
        endif()
    endif()
endfunction()

if(CINDER_USE_SPLIT_DWARF AND CMAKE_BUILD_TYPE MATCHES "Debug|RelWithDebInfo")
    message(STATUS "Cinder: split-dwarf enabled")
endif()

if(CINDER_UNITY_BUILD)
    set(CMAKE_UNITY_BUILD ON)
    set(CINDER_UNITY_BATCH_SIZE 8 CACHE STRING
        "Number of TUs merged per unity batch (lower = less peak RSS)")
    set(CMAKE_UNITY_BUILD_BATCH_SIZE ${CINDER_UNITY_BATCH_SIZE} CACHE STRING
        "Number of TUs merged per unity batch" FORCE)
    message(STATUS
        "Cinder: unity builds enabled (batch size ${CINDER_UNITY_BATCH_SIZE})")
endif()

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
# Only enabled for GCC — Clang configurations intentionally keep building
# C++ deps from source for hermeticity.
#
# FIND_PACKAGE_ARGS and its clauses (NAMES <pkg>) must be emitted together
# or not at all: a bare `NAMES <pkg>` without the keyword is not a valid
# FetchContent_Declare argument and corrupts the declare (the stray words
# end up as submodule pathspecs in the generated git-clone script, failing
# the download step). Hence this helper builds the whole clause or nothing.
macro(_cinder_find_package_args out_var package_name)
    if(CINDER_PREFER_SYSTEM_DEPS AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        # Optional extra args (e.g. a minimum version) are forwarded to
        # find_package() ahead of NAMES.
        set(${out_var} FIND_PACKAGE_ARGS ${ARGN} NAMES ${package_name})
    else()
        set(${out_var} "")
    endif()
endmacro()

cinder_use_cached_dep(asio)
FetchContent_Declare(
    asio
    GIT_REPOSITORY ${CINDER_DEP_asio_REPO}
    GIT_TAG        ${CINDER_DEP_asio_TAG}
    GIT_SHALLOW    TRUE
    GIT_SUBMODULES ""
    BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/asio-build
    SYSTEM
)
FetchContent_MakeAvailable(asio)
add_library(asio INTERFACE)
target_include_directories(asio SYSTEM INTERFACE ${asio_SOURCE_DIR}/asio/include)
target_compile_definitions(asio INTERFACE
    ASIO_STANDALONE
    ASIO_NO_DEPRECATED
)

cinder_use_cached_dep(xxhash)
FetchContent_Declare(
    xxhash
    GIT_REPOSITORY ${CINDER_DEP_xxhash_REPO}
    GIT_TAG        ${CINDER_DEP_xxhash_TAG}
    SOURCE_SUBDIR  cmake_unofficial
    GIT_SHALLOW    TRUE
    GIT_SUBMODULES ""
    BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/xxhash-build
    SYSTEM
)
FetchContent_MakeAvailable(xxhash)

_cinder_find_package_args(_spdlog_args spdlog 2)
cinder_use_cached_dep(spdlog PACKAGE spdlog VERSION 2)
FetchContent_Declare(
    spdlog
    GIT_REPOSITORY ${CINDER_DEP_spdlog_REPO}
    GIT_TAG        ${CINDER_DEP_spdlog_TAG}
    GIT_SHALLOW    TRUE
    GIT_SUBMODULES ""
    BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/spdlog-build
    SYSTEM
    ${_spdlog_args}
)
# NOTE: SPDLOG_BUILD_SHARED is deliberately NOT forced here. xxHash's
# cmake_unofficial declares option(BUILD_SHARED_LIBS ON), which seeds the
# cache before spdlog configures, so spdlog v2.x (which follows
# BUILD_SHARED_LIBS and offers no force-static switch) builds shared — as do
# grpc/abseil. Forcing static would additionally require
# POSITION_INDEPENDENT_CODE for PIE links; revisit only if static
# third-party linkage becomes an explicit goal.
FetchContent_MakeAvailable(spdlog)

_cinder_find_package_args(_cli11_args CLI11)
cinder_use_cached_dep(cli11 PACKAGE CLI11)
FetchContent_Declare(
    CLI11
    GIT_REPOSITORY ${CINDER_DEP_cli11_REPO}
    GIT_TAG        ${CINDER_DEP_cli11_TAG}
    GIT_SHALLOW    TRUE
    GIT_SUBMODULES ""
    BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/cli11-build
    SYSTEM
    ${_cli11_args}
)
FetchContent_MakeAvailable(CLI11)

_cinder_find_package_args(_yaml_cpp_args yaml-cpp)
cinder_use_cached_dep(yaml-cpp PACKAGE yaml-cpp)
FetchContent_Declare(
    yaml-cpp
    GIT_REPOSITORY ${CINDER_DEP_yaml-cpp_REPO}
    GIT_TAG        ${CINDER_DEP_yaml-cpp_TAG}
    GIT_SHALLOW    TRUE
    GIT_SUBMODULES ""
    BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/yaml-cpp-build
    SYSTEM
    ${_yaml_cpp_args}
)
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml-cpp)
if(TARGET yaml-cpp)
    target_compile_options(yaml-cpp PRIVATE -w)
endif()

if(CINDER_USE_MIMALLOC AND (CINDER_ENABLE_ASAN OR CINDER_ENABLE_TSAN OR CINDER_ENABLE_MSAN))
    message(STATUS "Cinder: mimalloc disabled — sanitizer build takes precedence")
    set(CINDER_USE_MIMALLOC OFF)
endif()

if(CINDER_USE_MIMALLOC)
    cinder_use_cached_dep(mimalloc)
    FetchContent_Declare(
        mimalloc
        GIT_REPOSITORY ${CINDER_DEP_mimalloc_REPO}
        GIT_TAG        ${CINDER_DEP_mimalloc_TAG}
        GIT_SHALLOW    TRUE
        GIT_SUBMODULES ""
        BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/mimalloc-build
        SYSTEM
    )

    set(MI_BUILD_TESTS  OFF CACHE BOOL "" FORCE)
    set(MI_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(MI_BUILD_OBJECT OFF CACHE BOOL "" FORCE)
    set(MI_BUILD_STATIC ON  CACHE BOOL "" FORCE)
    set(MI_OVERRIDE     ON  CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(mimalloc)
    message(STATUS "Cinder: mimalloc enabled (static override)")
endif()

if(CINDER_ENABLE_TLS)
    set(CINDER_ENABLE_CRYPTO ON CACHE BOOL "" FORCE)
    find_package(OpenSSL REQUIRED)
    message(STATUS "Cinder: TLS enabled, using OpenSSL ${OPENSSL_VERSION}")
elseif(CINDER_ENABLE_CRYPTO)
    find_package(OpenSSL REQUIRED COMPONENTS Crypto)
    message(STATUS "Cinder: Crypto enabled (HMAC-SHA256), using OpenSSL ${OPENSSL_VERSION}")
endif()

if(CINDER_BUILD_TESTS)
    _cinder_find_package_args(_gtest_args GTest)
    cinder_use_cached_dep(googletest PACKAGE GTest)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY ${CINDER_DEP_googletest_REPO}
        GIT_TAG        ${CINDER_DEP_googletest_TAG}
        GIT_SHALLOW    TRUE
        GIT_SUBMODULES ""
        BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/googletest-build
        SYSTEM
        EXCLUDE_FROM_ALL
        ${_gtest_args}
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
    if(TARGET gtest AND TARGET gtest_main)
        target_compile_options(gtest PRIVATE -w)
        target_compile_options(gtest_main PRIVATE -w)
    endif()
endif()

if(CINDER_BUILD_BENCHMARKS)
    _cinder_find_package_args(_benchmark_args benchmark)
    cinder_use_cached_dep(benchmark PACKAGE benchmark)
    FetchContent_Declare(
        benchmark
        GIT_REPOSITORY ${CINDER_DEP_benchmark_REPO}
        GIT_TAG        ${CINDER_DEP_benchmark_TAG}
        GIT_SHALLOW    TRUE
        GIT_SUBMODULES ""
        BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/benchmark-build
        SYSTEM
        EXCLUDE_FROM_ALL
        ${_benchmark_args}
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

# ---------------------------------------------------------------------------
# gRPC gateway (opt-in via CINDER_ENABLE_GRPC)
# ---------------------------------------------------------------------------
if(CINDER_ENABLE_GRPC)
    set(gRPC_BUILD_TESTS          OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_CODEGEN        ON  CACHE BOOL "" FORCE)
    set(gRPC_BUILD_GRPC_CPP_PLUGIN ON CACHE BOOL "" FORCE)
    set(gRPC_BUILD_CPP_PLUGIN     OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_CSHARP_EXT     OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_NODE_EXT       OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_PYTHON_EXT     OFF CACHE BOOL "" FORCE)
    set(gRPC_BUILD_RUBY_EXT       OFF CACHE BOOL "" FORCE)
    set(gRPC_INSTALL              OFF CACHE BOOL "" FORCE)
    set(gRPC_SSL_PROVIDER         package CACHE STRING "" FORCE)
    set(protobuf_BUILD_TESTS      OFF CACHE BOOL "" FORCE)
    set(protobuf_INSTALL           OFF CACHE BOOL "" FORCE)
    set(absl_BUILD_TESTING        OFF CACHE BOOL "" FORCE)
    set(absl_PROPAGATE_CXX_STD    ON  CACHE BOOL "" FORCE)
    set(RE2_BUILD_TESTING         OFF CACHE BOOL "" FORCE)

    cinder_use_cached_dep(grpc)
    FetchContent_Declare(
        grpc
        GIT_REPOSITORY ${CINDER_DEP_grpc_REPO}
        GIT_TAG        ${CINDER_DEP_grpc_TAG}
        GIT_SHALLOW    TRUE
        BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/grpc-build
        SYSTEM
    )
    # Suppress compiler warnings from gRPC internal source code (deprecated
    # c-ares APIs, missing-return in debug-only switch arms, attribute warnings
    # on unions, etc.). These are gRPC bugs, not ours.
    set(_cinder_save_cxx_flags "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
    FetchContent_MakeAvailable(grpc)
    set(CMAKE_CXX_FLAGS "${_cinder_save_cxx_flags}")

    # Compile proto/cinder/v1/cache.proto
    set(_proto_src_dir ${CMAKE_CURRENT_SOURCE_DIR}/cinder/v1)
    set(_proto_out_dir ${CMAKE_CURRENT_BINARY_DIR}/generated)
    file(MAKE_DIRECTORY ${_proto_out_dir})

    set(_proto_file ${_proto_src_dir}/cache.proto)
    add_custom_command(
        OUTPUT
            ${_proto_out_dir}/cache.pb.cc
            ${_proto_out_dir}/cache.pb.h
            ${_proto_out_dir}/cache.grpc.pb.cc
            ${_proto_out_dir}/cache.grpc.pb.h
        COMMAND $<TARGET_FILE:protoc>
            --cpp_out=${_proto_out_dir}
            --grpc_out=${_proto_out_dir}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:grpc_cpp_plugin>
            -I ${_proto_src_dir}
            ${_proto_file}
        DEPENDS ${_proto_file} protoc grpc_cpp_plugin
        COMMENT "Generating protobuf + gRPC code from cache.proto"
        VERBATIM
    )

    add_library(cinder_proto OBJECT
        ${_proto_out_dir}/cache.pb.cc
        ${_proto_out_dir}/cache.grpc.pb.cc
    )
    target_include_directories(cinder_proto PUBLIC ${_proto_out_dir})
    target_link_libraries(cinder_proto PUBLIC
        protobuf::libprotobuf
        grpc++
        grpc
    )
    target_compile_options(cinder_proto PRIVATE -w)

    # cinder_grpc: gateway library (links cinder_core + proto + grpc++)
    add_library(cinder_grpc OBJECT
        src/net/grpc_gateway.cpp
        src/net/cache_service_impl.cpp
    )
    target_include_directories(cinder_grpc PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${_proto_out_dir}
    )
    target_link_libraries(cinder_grpc PUBLIC cinder_core cinder_proto)
    target_compile_options(cinder_grpc PRIVATE -w)
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

if(CINDER_ENABLE_IWYU)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(WARNING "CINDER_ENABLE_IWYU works best with Clang; IWYU's own binary "
                "still requires a Clang-compatible AST regardless of the project's compiler")
    endif()
    find_program(IWYU_PROGRAM include-what-you-use)
    if(IWYU_PROGRAM)
        set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE "${IWYU_PROGRAM}")
        message(STATUS "Cinder: include-what-you-use enabled (${IWYU_PROGRAM})")
    else()
        message(WARNING "CINDER_ENABLE_IWYU is ON but include-what-you-use not found")
    endif()
endif()

# ---------------------------------------------------------------------------
# Core library
# ---------------------------------------------------------------------------
add_subdirectory(src)

add_library(cinder::core ALIAS cinder_core)

target_include_directories(cinder_core PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/third_party>
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
    target_compile_definitions(cinder_core PUBLIC CINDER_ENABLE_TLS CINDER_ENABLE_CRYPTO)
elseif(CINDER_ENABLE_CRYPTO)
    target_link_libraries(cinder_core PUBLIC OpenSSL::Crypto)
    target_compile_definitions(cinder_core PUBLIC CINDER_ENABLE_CRYPTO)
endif()

target_compile_options(cinder_core PRIVATE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion
    $<$<CONFIG:Release>:-O3>
    $<$<BOOL:${CINDER_ENABLE_COVERAGE}>:-fprofile-instr-generate;-fcoverage-mapping>
)
if(CINDER_ENABLE_COVERAGE)
    target_link_options(cinder_core PUBLIC -fprofile-instr-generate -fcoverage-mapping)
endif()
_cinder_apply_debug_options(cinder_core)

target_precompile_headers(cinder_core PRIVATE
    <asio.hpp>
    <spdlog/spdlog.h>
    <xxhash.h>
)

# Let other targets (tools, tests, benchmarks) reuse cinder_core's PCH instead
# of re-parsing asio/spdlog/xxhash. Call from their CMakeLists.txt right after
# add_executable()/add_library(). GCC rejects a PCH built with different
# optimisation flags (-Winvalid-pch: "__OPTIMIZE__ not defined"), so this also
# applies the same per-target debug/opt options cinder_core gets. Don't use it
# on targets with extra instrumentation flags (e.g. the libFuzzer targets).
function(cinder_reuse_pch target)
    if(NOT CMAKE_DISABLE_PRECOMPILE_HEADERS AND TARGET cinder_core)
        _cinder_apply_debug_options(${target})
        target_precompile_headers(${target} REUSE_FROM cinder_core)
    endif()
endfunction()

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

find_program(CPPCHECK_PROGRAM cppcheck)
if(CPPCHECK_PROGRAM)
    add_custom_target(cppcheck
        COMMAND ${CPPCHECK_PROGRAM} --enable=warning,performance,portability
                --std=c++23 --language=c++ --error-exitcode=1
                --project=${CMAKE_BINARY_DIR}/compile_commands.json
                --suppressions-list=${CMAKE_SOURCE_DIR}/suppressions/cppcheck.supp
                -i tests
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Running cppcheck static analysis"
    )
endif()

# Aggregate target for whichever of the above lint tools are actually
# available on this machine — `ninja lint` / `cmake --build . --target lint`
# runs all of them in one shot instead of needing to know each name.
set(_cinder_lint_deps "")
if(TARGET check-format)
    list(APPEND _cinder_lint_deps check-format)
endif()
if(TARGET cppcheck)
    list(APPEND _cinder_lint_deps cppcheck)
endif()
if(_cinder_lint_deps)
    add_custom_target(lint DEPENDS ${_cinder_lint_deps})
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
# Pre-commit hook auto-install
# ---------------------------------------------------------------------------
# Point core.hooksPath at .githooks so a fresh clone gets the fast lint checks
# without remembering `just hooks-install`. Guards, in order:
#   * opt-out via -DCINDER_AUTO_HOOKS=OFF (set once, sticky in the cache);
#   * only inside a real git work tree with .githooks/pre-commit present;
#   * only written when core.hooksPath is currently ABSENT, so a deliberate
#     `just hooks-uninstall` (which unsets it) or a user's own hooksPath is
#     never stomped. Note: `--get` exits nonzero for both an unset key and an
#     explicitly-empty one; we additionally require the key to exist, so an
#     empty value (the user clearing it on purpose) counts as "set" — leave it.
# All work is best-effort: a read-only or odd git config must never fail configure.
option(CINDER_AUTO_HOOKS "Install the repo pre-commit hook on configure" ON)
if(CINDER_AUTO_HOOKS AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.githooks/pre-commit")
    find_package(Git QUIET)
    if(GIT_FOUND)
        execute_process(
            COMMAND ${GIT_EXECUTABLE} rev-parse --is-inside-work-tree
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            OUTPUT_VARIABLE _cinder_in_worktree OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET RESULT_VARIABLE _cinder_git_rc
        )
        if(_cinder_git_rc EQUAL 0 AND _cinder_in_worktree STREQUAL "true")
            # --get-all lists every value; empty output means the key is absent.
            execute_process(
                COMMAND ${GIT_EXECUTABLE} config --local --get-all core.hooksPath
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                OUTPUT_VARIABLE _cinder_hooks_paths ERROR_QUIET
            )
            if(_cinder_hooks_paths STREQUAL "")
                execute_process(
                    COMMAND ${GIT_EXECUTABLE} config --local --add core.hooksPath
                            "${CMAKE_CURRENT_SOURCE_DIR}/.githooks"
                    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                    ERROR_QUIET RESULT_VARIABLE _cinder_hook_rc
                )
                if(_cinder_hook_rc EQUAL 0)
                    message(STATUS "Cinder: pre-commit hook installed (core.hooksPath=.githooks)")
                else()
                    message(STATUS "Cinder: could not install pre-commit hook — run `just hooks-install`")
                endif()
            endif()
        endif()
    endif()
endif()
