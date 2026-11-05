# Developer lint targets: clang-format check and cppcheck, aggregated under
# `lint`. Each tool contributes only if its binary is found, so `ninja lint`
# runs all of them in one shot instead of needing to know each name.

include_guard(GLOBAL)

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
# available on this machine.
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
