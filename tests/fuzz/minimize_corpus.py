#!/usr/bin/env python3
"""Minimize fuzz corpus to smallest set maintaining coverage.

Algorithm (greedy):
1. Load per-seed coverage from merged profdata
2. For each seed, compute which source lines it covers
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
import os
import subprocess
import sys
from pathlib import Path


def find_llvm_tool(name: str) -> str:
    """Find llvm-cov, trying versioned names first."""
    for tool in [name, f"{name}-19", f"{name}-18", f"{name}-17"]:
        path = subprocess.run(["which", tool], capture_output=True, text=True)
        if path.returncode == 0:
            return path.stdout.strip()
    print(f"ERROR: {name} not found in PATH", file=sys.stderr)
    sys.exit(1)


def get_seed_coverage(fuzzer: Path, seed: Path, profdata: Path, llvm_cov: str) -> set:
    """Get the set of source lines covered by a single seed."""
    env = os.environ.copy()
    env["LLVM_PROFILE_FILE"] = str(profdata)

    try:
        # Use llvm-cov to show coverage for this specific seed
        result = subprocess.run(
            [
                llvm_cov, "show",
                "--instr-profile", str(profdata),
                "--sources", "src/",
                str(fuzzer),
                "--", str(seed),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )

        # Parse output to extract covered lines
        covered = set()
        for line in result.stdout.splitlines():
            # Lines like "  42 |    code here" have a line number
            parts = line.split("|")
            if len(parts) >= 2:
                line_num = parts[0].strip()
                if line_num.isdigit():
                    covered.add(int(line_num))
        return covered
    except Exception:
        return set()


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

    # Get total coverage from merged profdata
    result = subprocess.run(
        [llvm_cov, "report", "--instr-profile", str(profdata), str(fuzzer)],
        capture_output=True,
        text=True,
    )
    print("Total coverage:")
    print(result.stdout)

    # Collect per-seed coverage
    seeds = sorted(corpus_dir.iterdir())
    seeds = [s for s in seeds if s.is_file()]
    print(f"\nAnalyzing {len(seeds)} seeds...")

    seed_coverage = {}
    for i, seed in enumerate(seeds, 1):
        print(f"  [{i}/{len(seeds)}] {seed.name}...", end=" ", flush=True)
        coverage = get_seed_coverage(fuzzer, seed, profdata, llvm_cov)
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
