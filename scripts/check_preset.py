"""Validate that a CMake preset exists for configure or ctest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("configure", "test"))
    parser.add_argument("name")
    args = parser.parse_args()

    with (ROOT / "CMakePresets.json").open(encoding="utf-8") as stream:
        config = json.load(stream)
    if args.kind == "configure":
        available = {
            entry["name"]
            for entry in config.get("configurePresets", [])
            if not entry.get("hidden", False)
        }
    else:
        available = {
            entry["name"]
            for entry in config.get("testPresets", [])
            if not entry.get("hidden", False) and "configurePreset" in entry
        }

    if args.name in available:
        return

    print(f"error: '{args.name}' is not a {args.kind} preset in CMakePresets.json.", file=sys.stderr)
    if args.kind == "test":
        print("       It has no ctest preset (build/lint-only), so tests cannot run on it.", file=sys.stderr)
    print(f"Available {args.kind} presets:", file=sys.stderr)
    for name in sorted(available):
        print(f"  {name}", file=sys.stderr)
    raise SystemExit(1)


if __name__ == "__main__":
    main()
