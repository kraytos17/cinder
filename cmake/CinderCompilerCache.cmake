# Compiler cache (ccache/sccache) and precompiled-header flags.
#
# Included from the top-level CMakeLists.txt after the options. Sets
# CMAKE_CXX/C_COMPILER_LAUNCHER when a cache tool is found and decides whether
# cinder_core's PCH stays enabled. The ccache settings travel through the
# launcher (cmake -E env) rather than the user's ccache.conf so they apply
# identically in every preset, under `just`, in CI and in IDEs. NB: a CMake
# variable named CCACHE_BASEDIR does nothing — ccache only reads the
# environment variable / config key.

include_guard(GLOBAL)

# ccache is preferred over sccache: sccache cannot handle GCC precompiled
# headers (cinder_core uses a PCH), ccache can once it is configured for PCH.
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
