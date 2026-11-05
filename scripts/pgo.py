"""Small safety checks for Cinder's GCC PGO workflow."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def data_dir(base: Path = ROOT) -> Path:
    return base / "build" / "pgo-data"


def build_dir(base: Path = ROOT) -> Path:
    return base / "build" / "pgo"


def has_profiles(base: Path = ROOT) -> bool:
    data = data_dir(base)
    return data.is_dir() and any(data.rglob("*.gcda"))


def clean(base: Path = ROOT) -> None:
    for path in (build_dir(base), data_dir(base)):
        if path.exists():
            shutil.rmtree(path)


def check(base: Path = ROOT) -> None:
    if not has_profiles(base):
        print("error: no .gcda profiles in build/pgo-data — run: just pgo-train", file=sys.stderr)
        raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("clean", "check"))
    action = parser.parse_args().action
    if action == "clean":
        clean()
    else:
        check()


if __name__ == "__main__":
    main()
