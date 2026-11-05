# Fast linker selection (mold, then lld, else the default).
#
# NB: GCC LTO emits slim bytecode objects that need the linker plugin. This
# project originally saw `undefined symbol: main` when combining them with
# mold/lld, so the combination is gated out by default for exactly GNU+LTO.
# Current mold supports GCC LTO through the standard linker-plugin interface
# (lld does not), so retest with -DCINDER_FAST_LINKER_WITH_GCC_LTO=ON and
# flip the default if it links. Clang LTO (LLVM bitcode) works with both.

include_guard(GLOBAL)

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
