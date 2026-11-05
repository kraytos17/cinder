# Pre-commit hook auto-install: point core.hooksPath at .githooks so a fresh
# clone gets the fast lint checks without remembering `just hooks-install`.
# Guards, in order:
#   * opt-out via -DCINDER_AUTO_HOOKS=OFF (set once, sticky in the cache);
#   * only inside a real git work tree with .githooks/pre-commit present;
#   * only written when core.hooksPath is currently ABSENT, so a deliberate
#     `just hooks-uninstall` (which unsets it) or a user's own hooksPath is
#     never stomped. Note: `--get` exits nonzero for both an unset key and an
#     explicitly-empty one; we additionally require the key to exist, so an
#     empty value (the user clearing it on purpose) counts as "set" — leave it.
# All work is best-effort: a read-only or odd git config must never fail configure.

include_guard(GLOBAL)

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
