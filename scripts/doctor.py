"""Check Cinder's required build tools and report optional developer tools."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def row(status: str, name: str, description: str) -> None:
    print(f"  {status:<9} {name:<22} {description}")


def version_tuple(value: str) -> tuple[int, ...]:
    parts: list[int] = []
    for part in value.split("."):
        digits = "".join(char for char in part if char.isdigit())
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)


def main() -> None:
    missing = 0

    print("Required:")
    required = {
        "cmake": "build system (CMakeLists.txt needs >= 4.4)",
        "ninja": "generator used by every preset",
        "git": "FetchContent clones + pre-commit hook",
        "bash": "recipe shell",
        "python3": "developer scripts, fuzz corpus, and profiling helpers",
    }
    for tool, description in required.items():
        found = shutil.which(tool) is not None
        row("ok" if found else "MISSING", tool, description)
        missing += not found

    cmake = shutil.which("cmake")
    if cmake:
        output = subprocess.run(
            [cmake, "--version"], capture_output=True, text=True, check=False
        ).stdout
        version = output.split("version", 1)[-1].strip().splitlines()[0]
        if version_tuple(version) >= (4, 4):
            row("ok", f"cmake {version}", ">= 4.4")
        else:
            row("MISSING", f"cmake {version}", "too old: need >= 4.4")
            missing += 1

    for toolchain in ("toolchains/gcc16.cmake", "toolchains/clang22.cmake"):
        exists = (ROOT / toolchain).is_file()
        row("ok" if exists else "MISSING", toolchain, "referenced by CMakePresets.json")
        missing += not exists
    print("  (compilers come from those toolchain files: GCC >= 16 / Clang >= 22)")

    optional = {
        "ccache": "compiler cache (preferred; the only one that handles GCC PCH)",
        "sccache": "compiler cache fallback",
        "mold": "fast linker (preferred)",
        "ld.lld": "fast linker fallback",
        "clang-format": "format / check-format / lint",
        "clang-tidy": "lint (ci presets)",
        "cppcheck": "cppcheck / lint",
        "include-what-you-use": "iwyu preset",
        "buf": "proto-* recipes",
        "llvm-cov": "fuzz-coverage-report",
        "samply": "samply-repl",
        "perf": "perf-stat-repl",
    }
    print("Optional (CMake falls back gracefully or matching recipes are unavailable):")
    for tool, description in optional.items():
        row("ok" if shutil.which(tool) else "absent", tool, description)

    if missing:
        print(f"\n{missing} required item(s) missing.", file=sys.stderr)
        raise SystemExit(1)
    print("\nAll required tools present.")


if __name__ == "__main__":
    main()
