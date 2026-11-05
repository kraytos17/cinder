"""Print Cinder configure presets, test availability, and build directories."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def presets() -> dict[str, dict[str, object]]:
    with (ROOT / "CMakePresets.json").open(encoding="utf-8") as stream:
        config = json.load(stream)
    return {
        entry["name"]: entry
        for entry in config.get("configurePresets", [])
        if not entry.get("hidden", False)
    }


def binary_directory(
    name: str,
    entries: dict[str, dict[str, object]],
    leaf_name: str | None = None,
) -> Path:
    """Resolve CMake's inherited binaryDir, including Cinder's substitutions."""
    if leaf_name is None:
        leaf_name = name
    entry = entries.get(name)
    if entry is None:
        raise SystemExit(f"error: unknown configure preset: {name}")
    binary_dir = entry.get("binaryDir")
    if binary_dir is None:
        parent = entry.get("inherits")
        if isinstance(parent, str) and parent in entries:
            return binary_directory(parent, entries, leaf_name)
        if isinstance(parent, list):
            for parent_name in parent:
                if parent_name in entries:
                    return binary_directory(parent_name, entries, leaf_name)
        binary_dir = "${sourceDir}/build/${presetName}"
    resolved = (
        str(binary_dir)
        .replace("${sourceDir}", str(ROOT))
        .replace("${presetName}", leaf_name)
    )
    return Path(resolved)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("preset", nargs="?", default="debug")
    parser.add_argument("--build-dir", action="store_true", help="print the preset's resolved binaryDir")
    args = parser.parse_args()

    entries = presets()
    if args.build_dir:
        print(binary_directory(args.preset, entries).relative_to(ROOT))
        return
    with (ROOT / "CMakePresets.json").open(encoding="utf-8") as stream:
        config = json.load(stream)

    test_presets = {
        entry["configurePreset"]
        for entry in config.get("testPresets", [])
        if not entry.get("hidden", False) and "configurePreset" in entry
    }
    current = args.preset
    print(f"  {'PRESET':17} {'TESTS':6} {'BUILD-DIR':22} DESCRIPTION")
    for entry in config.get("configurePresets", []):
        if entry.get("hidden", False):
            continue
        name = entry["name"]
        build_dir = str(binary_directory(name, entries).relative_to(ROOT))
        has_tests = "yes" if name in test_presets else "no"
        mark = "*" if name == current else " "
        description = entry.get("description", "")
        print(f"{mark} {name:17} {has_tests:6} {build_dir:22} {description}")
    print("\nTESTS=no: build/lint-only (no ctest preset). Select with: just preset=<name> <recipe>")


if __name__ == "__main__":
    main()
