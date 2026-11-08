# Standard FetchContent declarations for all third-party dependencies.
#
# Every dependency shares the same Declare boilerplate (shallow clone, no
# submodules unless requested, per-preset binary dir, SYSTEM includes), so it
# lives in cinder_declare_dep() below. What stays in CMakeLists.txt per
# dependency: the FetchContent_MakeAvailable call plus any target tweaks
# (compile options, interface libraries, cache variables that must be set
# BEFORE MakeAvailable runs).
#
# Requires cmake/CinderDeps.cmake (versions + cinder_use_cached_dep); included
# here so this module is self-sufficient.

include_guard(GLOBAL)

include("${CMAKE_CURRENT_LIST_DIR}/CinderDeps.cmake")

# Common FIND_PACKAGE_ARGS toggle: when ON, each declare below tries
# find_package() first (system/vcpkg/conan install) and only falls back to
# cloning + building from source if nothing satisfying is found. Only enabled
# for GCC — Clang configurations intentionally keep building C++ deps from
# source for hermeticity.
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

# Declares one dependency with Cinder's standard FetchContent arguments.
# Follow with FetchContent_MakeAvailable(<name>) in CMakeLists.txt.
#
#   cinder_declare_dep(<FetchContent name> [KEY <lower-key>]
#       [PACKAGE <find_package name>] [VERSION <min version>]
#       [SOURCE_SUBDIR <dir>] [WITH_SUBMODULES] [EXCLUDE_FROM_ALL])
#
# KEY defaults to the lower-cased FetchContent name and selects the
# CINDER_DEP_<key>_REPO/TAG pin plus the _deps/<key>-build binary dir.
# WITH_SUBMODULES keeps the default submodule behavior (only gRPC needs it);
# every other dep passes GIT_SUBMODULES "".
function(cinder_declare_dep fc_name)
    cmake_parse_arguments(D "WITH_SUBMODULES;EXCLUDE_FROM_ALL" "KEY;PACKAGE;VERSION;SOURCE_SUBDIR" "" ${ARGN})
    if(NOT D_KEY)
        string(TOLOWER "${fc_name}" D_KEY)
    endif()

    if(D_PACKAGE)
        if(D_VERSION)
            _cinder_find_package_args(_fpa_args ${D_PACKAGE} ${D_VERSION})
        else()
            _cinder_find_package_args(_fpa_args ${D_PACKAGE})
        endif()
        cinder_use_cached_dep(${D_KEY} PACKAGE ${D_PACKAGE} VERSION ${D_VERSION})
    else()
        set(_fpa_args "")
        cinder_use_cached_dep(${D_KEY})
    endif()

    set(_args
        GIT_REPOSITORY ${CINDER_DEP_${D_KEY}_REPO}
        GIT_TAG        ${CINDER_DEP_${D_KEY}_TAG}
        GIT_SHALLOW    TRUE
        BINARY_DIR     ${CMAKE_BINARY_DIR}/_deps/${D_KEY}-build
        SYSTEM)
    if(NOT D_WITH_SUBMODULES)
        list(APPEND _args GIT_SUBMODULES "")
    endif()
    if(D_SOURCE_SUBDIR)
        list(APPEND _args SOURCE_SUBDIR ${D_SOURCE_SUBDIR})
    endif()
    if(D_EXCLUDE_FROM_ALL)
        list(APPEND _args EXCLUDE_FROM_ALL)
    endif()
    if(_fpa_args)
        list(APPEND _args ${_fpa_args})
    endif()
    FetchContent_Declare(${fc_name} ${_args})
endfunction()

# Declares, populates, and tweaks every third-party dependency. Called once
# from the top-level CMakeLists.txt after find_package(Threads) /
# include(FetchContent). Per-dependency cache variables must be set BEFORE
# the corresponding MakeAvailable — keep each set() directly above its call.
function(cinder_fetch_all_dependencies)
    cinder_declare_dep(asio)
    FetchContent_MakeAvailable(asio)
    add_library(asio INTERFACE)
    target_include_directories(asio SYSTEM INTERFACE ${asio_SOURCE_DIR}/asio/include)
    target_compile_definitions(asio INTERFACE
        ASIO_STANDALONE
        ASIO_NO_DEPRECATED
    )

    cinder_declare_dep(xxhash SOURCE_SUBDIR build/cmake)
    FetchContent_MakeAvailable(xxhash)

    # Only the static library; no programs, tests, contrib tools, or
    # legacy-format decoders (we only read frames we wrote).
    set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_CONTRIB OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)
    set(ZSTD_LEGACY_SUPPORT OFF CACHE BOOL "" FORCE)
    cinder_declare_dep(zstd SOURCE_SUBDIR build/cmake)
    FetchContent_MakeAvailable(zstd)
    # zstd's legacy decoders collide under unity builds; legacy is off
    # anyway, but keep this target out of jumbo builds regardless.
    set_target_properties(libzstd_static PROPERTIES UNITY_BUILD OFF)

    cinder_declare_dep(spdlog PACKAGE spdlog VERSION 2)
    # NOTE: SPDLOG_BUILD_SHARED is deliberately NOT forced here. xxHash's
    # build/cmake declares option(BUILD_SHARED_LIBS ON), which seeds the
    # cache before spdlog configures, so spdlog v2.x (which follows
    # BUILD_SHARED_LIBS and offers no force-static switch) builds shared — as do
    # grpc/abseil. Forcing static would additionally require
    # POSITION_INDEPENDENT_CODE for PIE links; revisit only if static
    # third-party linkage becomes an explicit goal.
    FetchContent_MakeAvailable(spdlog)

    # Precompiled static library instead of header-only: function bodies compile
    # once into the lib rather than into every tool TU. Header-only mode is
    # untouched.
    set(CLI11_PRECOMPILED ON CACHE BOOL "" FORCE)
    cinder_declare_dep(CLI11 PACKAGE CLI11)
    FetchContent_MakeAvailable(CLI11)

    cinder_declare_dep(yaml-cpp PACKAGE yaml-cpp)
    set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(yaml-cpp)
    if(TARGET yaml-cpp)
        target_compile_options(yaml-cpp PRIVATE -w)
    endif()

    if(CINDER_USE_MIMALLOC AND (CINDER_ENABLE_ASAN OR CINDER_ENABLE_TSAN OR CINDER_ENABLE_MSAN))
        message(STATUS "Cinder: mimalloc disabled — sanitizer build takes precedence")
        # Function scope: propagate so the cinder_core link block sees it.
        set(CINDER_USE_MIMALLOC OFF PARENT_SCOPE)
    endif()

    if(CINDER_USE_MIMALLOC)
        cinder_declare_dep(mimalloc)

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
        cinder_declare_dep(googletest PACKAGE GTest EXCLUDE_FROM_ALL)
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
        FetchContent_MakeAvailable(googletest)
        if(TARGET gtest AND TARGET gtest_main)
            target_compile_options(gtest PRIVATE -w)
            target_compile_options(gtest_main PRIVATE -w)
        endif()
    endif()

    if(CINDER_BUILD_BENCHMARKS)
        cinder_declare_dep(benchmark PACKAGE benchmark EXCLUDE_FROM_ALL)
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
endfunction()
