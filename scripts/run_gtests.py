"""Run a configured list of Cinder GoogleTest binaries in order."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GROUPS_FILE = ROOT / "scripts" / "test_binaries.json"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("build_dir", type=Path)
    parser.add_argument("label")
    with GROUPS_FILE.open(encoding="utf-8") as stream:
        groups = json.load(stream)
    parser.add_argument("group", choices=tuple(groups))
    parser.add_argument("extra", nargs=argparse.REMAINDER, help="arguments forwarded to each binary")
    args = parser.parse_args()

    extra = args.extra[1:] if args.extra[:1] == ["--"] else args.extra
    binaries = groups[args.group]
    for binary in binaries:
        print(f"\n==> {binary}...\n────────────────────────────────────────", flush=True)
        command = [
            str(args.build_dir / "tests" / binary),
            "--gtest_also_run_disabled_tests",
            *extra,
        ]
        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as error:
            print(f"\nFAILED: {binary} ({args.label})", file=sys.stderr)
            raise SystemExit(error.returncode) from error
        print(f"PASSED: {binary}")

    print(f"\n════════════════════════════════════════\nAll test suites passed ({args.label}).")


if __name__ == "__main__":
    main()
