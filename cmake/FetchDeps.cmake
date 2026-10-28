# Populate the shared dependency source cache from the command line:
#
#   cmake -P cmake/FetchDeps.cmake                          # all but gRPC
#   cmake -DCINDER_DEPS_WITH_GRPC=ON -P cmake/FetchDeps.cmake
#   cmake "-DCINDER_DEPS=asio;spdlog" -P cmake/FetchDeps.cmake
#   cmake -DCINDER_PRINT_DIR=ON -P cmake/FetchDeps.cmake    # print cache dir
#
# Cache location: -DCINDER_DEPS_CACHE=<dir>, $CINDER_DEPS_CACHE, or
# ${XDG_CACHE_HOME:-~/.cache}/cinder/deps.

cmake_minimum_required(VERSION 3.25)
include("${CMAKE_CURRENT_LIST_DIR}/CinderDeps.cmake")

if(CINDER_PRINT_DIR)
    cinder_deps_cache_dir(_dir)
    execute_process(COMMAND "${CMAKE_COMMAND}" -E echo "${_dir}")
    return()
endif()

if(CINDER_DEPS)
    set(_keys ${CINDER_DEPS})
else()
    set(_keys ${CINDER_DEP_KEYS})
    if(NOT CINDER_DEPS_WITH_GRPC)
        list(REMOVE_ITEM _keys grpc)
    endif()
endif()

set(_failed "")
foreach(_k IN LISTS _keys)
    cinder_populate_dep(${_k} _dir)
    if(NOT _dir)
        list(APPEND _failed ${_k})
    endif()
endforeach()

cinder_deps_cache_dir(_cache)
if(_failed)
    message(FATAL_ERROR "Cinder deps: failed to populate: ${_failed}")
endif()
message(STATUS "Cinder deps: cache ready at ${_cache}")
