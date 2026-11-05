"""Small safety checks for Cinder's GCC PGO workflow."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("clean", "check"))
    action = parser.parse_args().action
    data = ROOT / "build" / "pgo-data"
    build = ROOT / "build" / "pgo"

    if action == "clean":
        for path in (build, data):
            if path.exists():
                shutil.rmtree(path)
        return

    profiles = list(data.rglob("*.gcda")) if data.is_dir() else []
    if not profiles:
        print("error: no .gcda profiles in build/pgo-data — run: just pgo-train", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
