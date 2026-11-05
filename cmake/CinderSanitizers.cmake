# Instrumentation selection: fuzzer, coverage, and sanitizers.
#
# The fuzzer and coverage guards must resolve before the sanitizer flags are
# built below, since both force ASan on. Included from the top-level
# CMakeLists.txt; leaves SANITIZER_FLAGS / SANITIZER_LINK_FLAGS set for the
# frame-pointer rule at the end.

include_guard(GLOBAL)

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

# Compile-time ignorelist for sanitizers — global so it covers third-party
# deps built via FetchContent (xxHash globals, etc.). Must be set before
# cinder_fetch_all_dependencies() runs.
# Clang-only; GCC does not support -fsanitize-ignorelist.
set(CINDER_SANITIZER_IGNORELIST "${CMAKE_SOURCE_DIR}/suppressions/asan_ignorelist.txt")
if(EXISTS "${CINDER_SANITIZER_IGNORELIST}" AND CINDER_ENABLE_ASAN AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_compile_options(-fsanitize-ignorelist=${CINDER_SANITIZER_IGNORELIST})
    message(STATUS "Cinder: using sanitizer ignorelist")
endif()
