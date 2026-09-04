#!/usr/bin/env python3
"""Minimize fuzz corpus to smallest set maintaining coverage.

Algorithm (greedy):
1. For each seed, run the fuzzer to generate a per-seed profraw
2. Use llvm-cov export JSON to extract which source lines each seed covers
3. Greedily remove seeds that don't increase total coverage
4. Output minimal corpus

Usage:
    python3 tests/fuzz/minimize_corpus.py \
        --fuzzer build/fuzz-coverage/tests/protocol_decode_cov \
        --corpus tests/fuzz/corpus/protocol_decode \
        --profdata tests/fuzz/coverage/protocol_decode/merged.profdata \
        --output tests/fuzz/corpus_minimal/protocol_decode

Prerequisites:
    - Run collect_coverage.py first to generate merged.profdata
    - llvm-cov must be in PATH
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def find_llvm_tool(name: str) -> str:
    """Find llvm-cov, trying versioned names first."""
    for tool in [name, f"{name}-19", f"{name}-18", f"{name}-17"]:
        result = subprocess.run(["which", tool], capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
    print(f"ERROR: {name} not found in PATH", file=sys.stderr)
    sys.exit(1)


def get_seed_coverage(fuzzer: Path, seed: Path, llvm_cov: str, llvm_profdata: str, tmpdir: Path) -> set:
    """Run the fuzzer with a single seed and extract covered source lines."""
    profraw = tmpdir / f"{seed.name}.profraw"
    env = os.environ.copy()
    env["LLVM_PROFILE_FILE"] = str(profraw)

    try:
        # Pass the seed as a positional file argument with -runs=1 so the
        # fuzzer replays exactly this input once. Calling the binary with no
        # arguments starts libFuzzer's normal fuzzing loop (mutating its own
        # generated inputs indefinitely) — stdin is not read by libFuzzer
        # harnesses, so `input=seed_data` alone silently does nothing useful.
        subprocess.run(
            [str(fuzzer), str(seed), "-runs=1"],
            capture_output=True,
            timeout=10,
            env=env,
        )
    except subprocess.TimeoutExpired:
        pass
    except Exception:
        return set()

    if not profraw.exists():
        return set()

    # Merge single profraw into temp profdata
    profdata = tmpdir / f"{seed.name}.profdata"
    subprocess.run(
        [llvm_profdata, "merge", "-sparse", str(profraw), "-o", str(profdata)],
        capture_output=True,
    )

    if not profdata.exists():
        return set()

    # Get coverage JSON
    result = subprocess.run(
        [
            llvm_cov, "export",
            "--instr-profile", str(profdata),
            "--ignore-filename-regex", r"_deps/",
            str(fuzzer),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )

    if result.returncode != 0:
        return set()

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return set()

    # llvm-cov export's schema nests segments under files, not directly under
    # each "data" entry: data -> [ {files: [ {filename, segments: [...]}, ... ]} ].
    # Reading segments straight off the "data" entries (as before) always
    # returned an empty list, since that key lives one level deeper.
    # Line numbers are also namespaced by filename here, since two different
    # files can both have a "line 12" and those aren't the same covered region.
    covered = set()
    for export_entry in data.get("data", []):
        for file_entry in export_entry.get("files", []):
            filename = file_entry.get("filename", "")
            for seg in file_entry.get("segments", []):
                # seg = [line, col, count, hasCount, isRegionEntry, isGapRegion]
                if len(seg) >= 3 and seg[2] > 0:
                    covered.add((filename, seg[0]))

    return covered


def main():
    parser = argparse.ArgumentParser(description="Minimize corpus to smallest set maintaining coverage")
    parser.add_argument("--fuzzer", required=True, help="Path to coverage-instrumented fuzz binary")
    parser.add_argument("--corpus", required=True, help="Path to seed corpus directory")
    parser.add_argument("--profdata", required=True, help="Path to merged.profdata")
    parser.add_argument("--output", required=True, help="Output directory for minimal corpus")
    args = parser.parse_args()

    fuzzer = Path(args.fuzzer)
    corpus_dir = Path(args.corpus)
    profdata = Path(args.profdata)
    output_dir = Path(args.output)

    if not fuzzer.exists():
        print(f"ERROR: Fuzzer binary not found: {fuzzer}", file=sys.stderr)
        sys.exit(1)
    if not profdata.exists():
        print(f"ERROR: Profdata not found: {profdata}", file=sys.stderr)
        sys.exit(1)

    llvm_cov = find_llvm_tool("llvm-cov")
    llvm_profdata = find_llvm_tool("llvm-profdata")

    # Get total coverage from merged profdata
    result = subprocess.run(
        [llvm_cov, "report", "--instr-profile", str(profdata),
         "--ignore-filename-regex", r"_deps/", str(fuzzer)],
        capture_output=True,
        text=True,
    )
    print("Total coverage:")
    print(result.stdout)

    # Collect per-seed coverage
    seeds = sorted(corpus_dir.iterdir())
    seeds = [s for s in seeds if s.is_file()]
    print(f"\nAnalyzing {len(seeds)} seeds...")

    with tempfile.TemporaryDirectory(prefix="cinder-cov-") as tmpdir:
        tmpdir = Path(tmpdir)
        seed_coverage = {}
        for i, seed in enumerate(seeds, 1):
            print(f"  [{i}/{len(seeds)}] {seed.name}...", end=" ", flush=True)
            coverage = get_seed_coverage(fuzzer, seed, llvm_cov, llvm_profdata, tmpdir)
            seed_coverage[seed] = coverage
            print(f"{len(coverage)} lines")

    if not seed_coverage:
        print("ERROR: No coverage data collected", file=sys.stderr)
        sys.exit(1)

    # Greedy minimization
    print("\nMinimizing corpus...")
    all_lines = set()
    for coverage in seed_coverage.values():
        all_lines |= coverage

    print(f"Total unique lines covered: {len(all_lines)}")

    # Sort seeds by coverage (most first)
    sorted_seeds = sorted(seed_coverage.keys(), key=lambda s: len(seed_coverage[s]), reverse=True)

    minimal = []
    covered = set()
    for seed in sorted_seeds:
        new_lines = seed_coverage[seed] - covered
        if new_lines:
            minimal.append(seed)
            covered |= new_lines

    # Copy minimal corpus
    output_dir.mkdir(parents=True, exist_ok=True)
    for seed in minimal:
        dest = output_dir / seed.name
        dest.write_bytes(seed.read_bytes())

    print(f"\nMinimal corpus: {len(minimal)}/{len(seeds)} seeds ({100*len(minimal)//len(seeds)}% reduction)")
    print(f"Coverage maintained: {len(covered)}/{len(all_lines)} lines")
    print(f"Output: {output_dir}")


if __name__ == "__main__":
    main()
