#!/usr/bin/env python3
"""Collect per-seed coverage data for fuzz corpus minimization.

For each seed file in the corpus:
1. Run the coverage-instrumented fuzz target on that single seed
2. Collect the .profraw output
3. Merge into a combined .profdata

Usage:
    python3 tests/fuzz/collect_coverage.py \
        --fuzzer build/fuzz-coverage/tests/protocol_decode_cov \
        --corpus tests/fuzz/corpus/protocol_decode \
        --output tests/fuzz/coverage/protocol_decode

Prerequisites:
    - Build with: cmake --preset fuzz-coverage && cmake --build build/fuzz-coverage
    - llvm-profdata and llvm-cov must be in PATH
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def find_llvm_tool(name: str) -> str:
    """Find llvm-profdata or llvm-cov, trying versioned names first."""
    for tool in [name, f"{name}-19", f"{name}-18", f"{name}-17"]:
        path = subprocess.run(["which", tool], capture_output=True, text=True)
        if path.returncode == 0:
            return path.stdout.strip()
    print(f"ERROR: {name} not found in PATH", file=sys.stderr)
    sys.exit(1)


def collect_seed_coverage(fuzzer: Path, seed: Path, output_dir: Path, profdata: str) -> bool:
    """Run the fuzzer on a single seed and collect coverage."""
    env = os.environ.copy()
    # Use absolute path for LLVM_PROFILE_FILE
    env["LLVM_PROFILE_FILE"] = str((output_dir / profdata).resolve())

    try:
        result = subprocess.run(
            [str(fuzzer), str(seed), "-runs=1", "-max_total_time=1"],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        return False
    except Exception as e:
        print(f"  WARNING: {seed.name}: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description="Collect per-seed coverage data")
    parser.add_argument("--fuzzer", required=True, help="Path to coverage-instrumented fuzz binary")
    parser.add_argument("--corpus", required=True, help="Path to seed corpus directory")
    parser.add_argument("--output", required=True, help="Output directory for coverage data")
    args = parser.parse_args()

    fuzzer = Path(args.fuzzer)
    corpus_dir = Path(args.corpus)
    output_dir = Path(args.output)

    if not fuzzer.exists():
        print(f"ERROR: Fuzzer binary not found: {fuzzer}", file=sys.stderr)
        sys.exit(1)
    if not corpus_dir.exists():
        print(f"ERROR: Corpus directory not found: {corpus_dir}", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Find llvm-profdata
    profdata_tool = find_llvm_tool("llvm-profdata")

    # Clean stale .profraw from a previous run — a crash mid-collection (ASan
    # is implied by CINDER_ENABLE_COVERAGE) can leave a truncated/corrupt file
    # that would otherwise make llvm-profdata merge fail on the *next* run.
    for stale in output_dir.glob("*.profraw"):
        stale.unlink()

    # Collect seeds
    seeds = sorted(corpus_dir.iterdir())
    seeds = [s for s in seeds if s.is_file()]
    print(f"Collecting coverage for {len(seeds)} seeds...")

    profraw_files = []
    for i, seed in enumerate(seeds, 1):
        profraw_name = f"seed_{i:04d}_{seed.name}.profraw"
        print(f"  [{i}/{len(seeds)}] {seed.name}...", end=" ", flush=True)

        if collect_seed_coverage(fuzzer, seed, output_dir, profraw_name):
            profraw_path = output_dir / profraw_name
            if profraw_path.exists():
                profraw_files.append(profraw_path)
                print("OK")
            else:
                print("no profraw")
        else:
            print("FAILED")

    if not profraw_files:
        print("ERROR: No coverage data collected", file=sys.stderr)
        sys.exit(1)

    # Merge profraw files
    merged = output_dir / "merged.profdata"
    print(f"\nMerging {len(profraw_files)} profraw files...")
    cmd = [
        profdata_tool, "merge", "-sparse",
        "-output", str(merged),
    ] + [str(f) for f in profraw_files]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"ERROR: llvm-profdata merge failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)

    print(f"Merged profile written to {merged}")
    print(f"Coverage data collected for {len(profraw_files)}/{len(seeds)} seeds")


if __name__ == "__main__":
    main()
