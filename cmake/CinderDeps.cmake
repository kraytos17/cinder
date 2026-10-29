# Shared, pinned dependency *source* cache.
#
# Why: FetchContent clones every dependency into every build tree (and the old
# per-compiler FETCHCONTENT_BASE_DIR cloned each one twice: gcc + clang).
# Dependency sources do not depend on the compiler or the preset, so we keep ONE
# checkout per (dependency, tag) outside the build trees and hand it to
# FetchContent through FETCHCONTENT_SOURCE_DIR_<NAME>. When that variable is set,
# FetchContent performs no download/update for that dependency. Binary dirs stay
# per-preset (BINARY_DIR in each FetchContent_Declare), so sanitizer/compiler
# flags never collide.
#
# Usable from project mode (CMakeLists.txt) and script mode (FetchDeps.cmake):
# it only uses commands available in both.

include_guard(GLOBAL)

include("${CMAKE_CURRENT_LIST_DIR}/CinderDepVersions.cmake")

# Resolve the cache directory: -DCINDER_DEPS_CACHE, then $CINDER_DEPS_CACHE,
# then $XDG_CACHE_HOME/cinder/deps, then ~/.cache/cinder/deps.
function(cinder_deps_cache_dir out_var)
    if(DEFINED CINDER_DEPS_CACHE AND NOT "${CINDER_DEPS_CACHE}" STREQUAL "")
        set(_d "${CINDER_DEPS_CACHE}")
    elseif(NOT "$ENV{CINDER_DEPS_CACHE}" STREQUAL "")
        set(_d "$ENV{CINDER_DEPS_CACHE}")
    elseif(NOT "$ENV{XDG_CACHE_HOME}" STREQUAL "")
        set(_d "$ENV{XDG_CACHE_HOME}/cinder/deps")
    elseif(NOT "$ENV{HOME}" STREQUAL "")
        set(_d "$ENV{HOME}/.cache/cinder/deps")
    else()
        set(_d "${CMAKE_CURRENT_LIST_DIR}/../.cache/deps")
    endif()
    cmake_path(ABSOLUTE_PATH _d NORMALIZE)
    set(${out_var} "${_d}" PARENT_SCOPE)
endfunction()

# Run git; sets <rc_var> in the caller. Output is captured so a failure can be
# reported.
function(_cinder_git rc_var workdir)
    execute_process(
        COMMAND "${CINDER_GIT_EXECUTABLE}" -c advice.detachedHead=false ${ARGN}
        WORKING_DIRECTORY "${workdir}"
        RESULT_VARIABLE _rc
        OUTPUT_VARIABLE _out
        ERROR_VARIABLE  _err
        ERROR_STRIP_TRAILING_WHITESPACE)
    if(NOT _rc EQUAL 0)
        message(STATUS "Cinder deps: git ${ARGN} failed (${_rc}): ${_err}")
    endif()
    set(${rc_var} ${_rc} PARENT_SCOPE)
endfunction()

# Verify a cached checkout: the marker must record this exact repo and tag.
# Guards against hand-edited or corrupted cache entries being trusted blindly
# (a mismatch falls through to a clean re-fetch below).
function(_cinder_cache_valid dir repo tag out_var)
    set(${out_var} FALSE PARENT_SCOPE)
    set(_marker "${dir}/.cinder-populated")
    if(NOT EXISTS "${_marker}")
        return()
    endif()
    file(READ "${_marker}" _content LIMIT 4096)
    string(STRIP "${_content}" _content)
    if(_content STREQUAL "${repo} ${tag}")
        set(${out_var} TRUE PARENT_SCOPE)
    else()
        message(STATUS "Cinder deps: cached ${dir} marker mismatch, refetching")
    endif()
endfunction()

# Populate <cache>/<key>-<tag> if missing. Sets <out_var> to the directory on
# success, or "" on failure. Safe against concurrent configures/processes
# (file lock) and against interrupted runs (clone into .partial, then rename).
function(cinder_populate_dep key out_var)
    set(${out_var} "" PARENT_SCOPE)

    if(NOT DEFINED CINDER_DEP_${key}_REPO OR NOT DEFINED CINDER_DEP_${key}_TAG)
        message(FATAL_ERROR "Cinder deps: unknown dependency '${key}' (see cmake/CinderDepVersions.cmake)")
    endif()
    set(_repo "${CINDER_DEP_${key}_REPO}")
    set(_tag  "${CINDER_DEP_${key}_TAG}")

    cinder_deps_cache_dir(_cache)
    set(_dir "${_cache}/${key}-${_tag}")

    _cinder_cache_valid("${_dir}" "${_repo}" "${_tag}" _valid)
    if(_valid)
        set(${out_var} "${_dir}" PARENT_SCOPE)
        return()
    endif()

    if(NOT CINDER_GIT_EXECUTABLE)
        find_program(CINDER_GIT_EXECUTABLE git)
    endif()
    if(NOT CINDER_GIT_EXECUTABLE)
        message(STATUS "Cinder deps: git not found, cannot populate ${key}")
        return()
    endif()

    file(MAKE_DIRECTORY "${_cache}")
    file(LOCK "${_cache}/${key}.lock" GUARD FUNCTION TIMEOUT 1800 RESULT_VARIABLE _lock_rc)
    if(NOT _lock_rc EQUAL 0)
        message(STATUS "Cinder deps: could not lock ${key} (${_lock_rc})")
        return()
    endif()

    # Another process may have finished while we waited for the lock.
    _cinder_cache_valid("${_dir}" "${_repo}" "${_tag}" _valid)
    if(_valid)
        set(${out_var} "${_dir}" PARENT_SCOPE)
        return()
    endif()

    message(STATUS "Cinder deps: fetching ${key} @ ${_tag} -> ${_dir}")
    set(_tmp "${_dir}.partial")
    file(REMOVE_RECURSE "${_tmp}" "${_dir}")
    file(MAKE_DIRECTORY "${_tmp}")

    _cinder_git(_rc "${_tmp}" init -q)
    if(_rc EQUAL 0)
        _cinder_git(_rc "${_tmp}" remote add origin "${_repo}")
    endif()
    # Shallow fetch of a tag, branch or full commit SHA, with retries (the old
    # CMAKE_EP_GIT_CLONE_RETRY_* settings only applied to FetchContent clones).
    if(_rc EQUAL 0)
        foreach(_attempt RANGE 1 3)
            _cinder_git(_rc "${_tmp}" fetch -q --depth 1 origin "${_tag}")
            if(_rc EQUAL 0)
                break()
            endif()
            execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep 5)
        endforeach()
    endif()
    if(_rc EQUAL 0)
        _cinder_git(_rc "${_tmp}" checkout -q --detach FETCH_HEAD)
    endif()
    if(_rc EQUAL 0 AND CINDER_DEP_${key}_SUBMODULES)
        _cinder_git(_rc "${_tmp}" submodule update --init --recursive --depth 1 --jobs 8)
    endif()

    if(NOT _rc EQUAL 0)
        file(REMOVE_RECURSE "${_tmp}")
        message(STATUS "Cinder deps: failed to fetch ${key}")
        return()
    endif()

    file(WRITE "${_tmp}/.cinder-populated" "${_repo} ${_tag}\n")
    file(RENAME "${_tmp}" "${_dir}")
    set(${out_var} "${_dir}" PARENT_SCOPE)
endfunction()

# Project-mode entry point: call BEFORE FetchContent_Declare/MakeAvailable.
#
#   cinder_use_cached_dep(<key> [PACKAGE <find_package name> [VERSION <v>]])
#
# Sets FETCHCONTENT_SOURCE_DIR_<KEY> in the caller's scope when a cached
# checkout exists (or can be created and CINDER_DEPS_AUTOFETCH is ON). Does
# nothing when the user already set FETCHCONTENT_SOURCE_DIR_<KEY>, when the
# cache is disabled, or when the dependency is satisfied by find_package()
# (GCC + CINDER_PREFER_SYSTEM_DEPS): a source dir would otherwise bypass the
# FIND_PACKAGE_ARGS lookup and force a from-source build.
function(cinder_use_cached_dep key)
    cmake_parse_arguments(PARSE_ARGV 1 _A "" "PACKAGE;VERSION" "")
    string(TOUPPER "${key}" _u)

    if(NOT CINDER_USE_DEPS_CACHE OR DEFINED FETCHCONTENT_SOURCE_DIR_${_u})
        return()
    endif()

    if(_A_PACKAGE AND CINDER_PREFER_SYSTEM_DEPS AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        find_package(${_A_PACKAGE} ${_A_VERSION} QUIET)
        if(${_A_PACKAGE}_FOUND)
            message(STATUS "Cinder deps: ${key} provided by system package ${_A_PACKAGE}")
            return()
        endif()
    endif()

    cinder_deps_cache_dir(_cache)
    set(_dir "${_cache}/${key}-${CINDER_DEP_${key}_TAG}")
    _cinder_cache_valid(
        "${_dir}" "${CINDER_DEP_${key}_REPO}" "${CINDER_DEP_${key}_TAG}" _valid
    )
    if(NOT _valid)
        if(CINDER_DEPS_AUTOFETCH)
            cinder_populate_dep(${key} _dir)
        else()
            set(_dir "")
        endif()
    endif()

    if(_dir)
        set(FETCHCONTENT_SOURCE_DIR_${_u} "${_dir}" PARENT_SCOPE)
        message(STATUS "Cinder deps: ${key} -> ${_dir}")
    else()
        message(STATUS "Cinder deps: ${key} not cached, FetchContent will clone it into the build tree")
    endif()
endfunction()
