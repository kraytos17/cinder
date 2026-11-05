# Build tuning: linker choice is separate (cmake/CinderLinker.cmake); this
# module holds LTO, PGO, and the small compile/link knobs plus the
# _cinder_apply_debug_options() helper used by every cinder target (it must
# stay defined before add_subdirectory(src)).

include_guard(GLOBAL)

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
# PGO (GCC only): Generate instruments and writes .gcda into CINDER_PGO_DIR
# when the workload runs; Use rebuilds consuming that data. Both phases
# share one build dir (see the pgo-generate/pgo-use presets) so profile
# paths match exactly. Train with the throughput bench, then measure.
# ---------------------------------------------------------------------------
set(CINDER_PGO "OFF" CACHE STRING "Profile-guided optimization: OFF, Generate or Use (GCC only)")
set(CINDER_PGO_DIR "${CMAKE_BINARY_DIR}/../pgo-data" CACHE PATH "Directory for PGO profile data")
if(NOT CINDER_PGO STREQUAL "OFF")
    if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        message(FATAL_ERROR "Cinder: CINDER_PGO is GCC-only (got ${CMAKE_CXX_COMPILER_ID})")
    endif()
    file(MAKE_DIRECTORY "${CINDER_PGO_DIR}")
    if(CINDER_PGO STREQUAL "Generate")
        add_compile_options("-fprofile-generate=${CINDER_PGO_DIR}")
        add_link_options("-fprofile-generate=${CINDER_PGO_DIR}")
        message(STATUS "Cinder: PGO instrumentation -> ${CINDER_PGO_DIR}")
    elseif(CINDER_PGO STREQUAL "Use")
        add_compile_options("-fprofile-use=${CINDER_PGO_DIR}" -fprofile-correction -Wno-missing-profile)
        add_link_options("-fprofile-use=${CINDER_PGO_DIR}" -fprofile-correction)
        message(STATUS "Cinder: PGO consuming <- ${CINDER_PGO_DIR}")
    else()
        message(FATAL_ERROR "Cinder: CINDER_PGO must be OFF, Generate or Use (got ${CINDER_PGO})")
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
