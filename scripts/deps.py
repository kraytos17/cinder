"""Housekeeping for Cinder's external FetchContent source cache/build dirs."""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def cache_dir() -> Path:
    result = subprocess.run(
        ["cmake", "-DCINDER_PRINT_DIR=ON", "-P", "cmake/FetchDeps.cmake"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    raw_path = result.stdout.strip()
    if not raw_path:
        raise SystemExit("error: CMake returned an empty dependency-cache path")
    return Path(raw_path).expanduser()


def clean() -> None:
    configured_path = cache_dir()
    if configured_path.is_symlink():
        raise SystemExit(f"refusing to recursively delete symlinked cache path: {configured_path}")
    path = configured_path.resolve()
    home = Path.home().resolve()
    protected = {Path("/"), home, home / ".cache", ROOT}
    if not str(path) or path in protected or not path.name:
        raise SystemExit(f"refusing to delete suspicious cache path: {str(path)!r}")
    if not path.exists():
        print(f"cache does not exist: {path}")
        return
    shutil.rmtree(path)
    print(f"removed {path}")


def refresh() -> None:
    build_root = ROOT / "build"
    caches = sorted(build_root.glob("*/CMakeCache.txt"))
    for cache in caches:
        build_dir = cache.parent
        print(f"==> refreshing {build_dir.relative_to(ROOT)}...", flush=True)
        subprocess.run(
            ["cmake", "-U", "FETCHCONTENT_SOURCE_DIR_*", str(build_dir)],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            check=True,
        )
    print(
        f"done — {len(caches)} configured build dir(s) re-derived their source dirs "
        "from the current tags"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("clean", "refresh"))
    action = parser.parse_args().action
    if action == "clean":
        clean()
    else:
        refresh()


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        raise SystemExit(error.returncode) from error
