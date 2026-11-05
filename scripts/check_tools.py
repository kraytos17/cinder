"""Fail if any named executable is missing from PATH."""

from __future__ import annotations

import shutil
import sys


def main() -> None:
    missing = [tool for tool in sys.argv[1:] if shutil.which(tool) is None]
    if missing:
        print(f"error: not found on PATH: {' '.join(missing)}  (run: just doctor)", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
