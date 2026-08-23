# ---------------------------------------------------------------------------
# Cinder .gdbinit
# ---------------------------------------------------------------------------

# --- Usability defaults ---
set history save on
set history filename ~/.gdb_history
set history size 10000
set print pretty on
set print array-indexes on
set print elements 200
set print object on
set pagination off
set confirm off
set backtrace past-main off

# Persist parsed symbol data across sessions — meaningful win given
# -gsplit-dwarf: without this GDB re-parses .dwo indices every launch.
set index-cache enable on
set index-cache directory .cache/gdb-index

# Allow pending breakpoints so sanitizer symbols don't cause errors on
# non-sanitizer builds.
set breakpoint pending on

# --- Multi-threaded debugging ---
# Default OFF: only enable when single-stepping, otherwise other threads
# race ahead and can obscure what you're inspecting. Toggle with:
#   set scheduler-locking on
set scheduler-locking off

# --- Sanitizer interop ---
# When attaching to an ASan/UBSan-built binary, break here first so you can
# inspect state before the sanitizer's default abort() unwinds it away.
# These are set as pending — they silently resolve to nothing on non-sanitizer builds.
break __asan_report_error
break __ubsan_handle_type_mismatch_v1

# Point sanitizer runtimes at the suppression files already in the repo.
# GDB doesn't set these itself, but `run` inherits the shell environment —
# uncomment whichever build you're actually debugging:
# set environment ASAN_OPTIONS=suppressions=suppressions/asan.supp:abort_on_error=1
# set environment LSAN_OPTIONS=suppressions=suppressions/lsan.supp
# set environment TSAN_OPTIONS=suppressions=suppressions/tsan.supp:abort_on_error=1
# set environment UBSAN_OPTIONS=suppressions=suppressions/ubsan.supp:print_stacktrace=1

# mimalloc's own free-list structures look unfamiliar in a raw heap dump —
# when CINDER_USE_MIMALLOC is on and you need heap internals, `mi_*`
# debug helpers are more useful than GDB's default malloc-chunk assumptions.
# (No direct GDB equivalent needed here — noted so you don't assume glibc
# chunk headers when CINDER_USE_MIMALLOC=ON.)

define all-bt
    thread apply all bt
end
document all-bt
Backtrace every thread — first command to run when cinderd looks hung.
end

define all-bt-full
    thread apply all bt full
end
document all-bt-full
Full backtrace every thread including local variables — use for deep inspection.
end

define catch-throw
    catch throw
    commands
        silent
        bt 1
        continue
    end
end
document catch-throw
Break on every thrown exception, print a one-frame backtrace, and continue.
Useful for tracing exception flow without stopping.
end

# --- Load libstdc++ pretty printers if not auto-loaded by the distro ---
# On most systems GDB auto-loads these. If not, uncomment and adjust the path:
# python
# import sys, glob
# for p in glob.glob('/usr/share/gcc/*/python'):
#     sys.path.insert(0, p)
# from libstdcxx.v6.printers import register_libstdcxx_printers
# register_libstdcxx_printers(None)
# end
