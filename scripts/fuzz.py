"""Run Cinder libFuzzer targets and collect/minimize their coverage."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGETS_FILE = ROOT / "scripts" / "fuzz_targets.json"
DEFAULT_OPTIONS = ["-timeout=5"]


def targets() -> list[dict[str, object]]:
    with TARGETS_FILE.open(encoding="utf-8") as stream:
        return json.load(stream)


def run(command: list[str]) -> None:
    subprocess.run(command, cwd=ROOT, check=True)


def validate_target(name: str) -> bool:
    names = {entry["name"] for entry in targets()}
    if name in names:
        return True
    print(f"error: unknown fuzz target '{name}'. Known targets:", file=sys.stderr)
    for known in sorted(names):
        print(f"  {known}", file=sys.stderr)
    return False


def seed_corpus() -> None:
    if not (ROOT / "tests/fuzz/corpus/protocol_decode").is_dir():
        print("Generating fuzz corpus + dictionaries...", flush=True)
        run([sys.executable, "tests/fuzz/generate_corpus.py"])


def run_fuzzers(seconds: str, only: str, extra: list[str]) -> None:
    if only != "all" and not validate_target(only):
        raise SystemExit(2)

    seed_corpus()
    entries = targets()
    if only != "all":
        entries = [entry for entry in entries if entry["name"] == only]

    for entry in entries:
        name = entry["name"]
        options = list(entry["options"])
        for option in options.copy():
            if option.startswith("-dict=") and not (ROOT / option.removeprefix("-dict=")).is_file():
                print(f"note: {option.removeprefix('-dict=')} missing — running without a dictionary")
                options.remove(option)

        corpus = ROOT / "tests" / "fuzz" / "corpus" / name
        corpus.mkdir(parents=True, exist_ok=True)
        binary = ROOT / "build" / "fuzz" / "tests" / entry["binary"]
        command = [
            str(binary),
            f"-max_total_time={seconds}",
            "-print_final_stats=1",
            *DEFAULT_OPTIONS,
            *options,
            str(corpus),
            *extra,
        ]
        print(f"==> {name} ({seconds}s)...", flush=True)
        print("────────────────────────────────────────", flush=True)
        try:
            run(command)
        except subprocess.CalledProcessError as error:
            print(f"FAILED: {name}", file=sys.stderr)
            raise SystemExit(error.returncode) from error
        print(f"PASSED: {name}")

    print("════════════════════════════════════════")
    if only == "all":
        print(f"All fuzz targets passed ({seconds}s each).")
    else:
        print(f"Fuzz target {only} passed ({seconds}s).")


def require_target(name: str) -> dict[str, object]:
    for entry in targets():
        if entry["name"] == name:
            return entry
    validate_target(name)
    raise SystemExit(2)


def merge_corpus(name: str) -> None:
    require_target(name)
    minimal = ROOT / "tests" / "fuzz" / "corpus_minimal" / name
    minimal.mkdir(parents=True, exist_ok=True)
    run(
        [
            str(ROOT / "build" / "fuzz" / "tests" / require_target(name)["binary"]),
            "-merge=1",
            str(minimal),
            str(ROOT / "tests" / "fuzz" / "corpus" / name),
        ]
    )


def coverage_collect(name: str) -> None:
    require_target(name)
    run(
        [
            sys.executable,
            "tests/fuzz/collect_coverage.py",
            "--fuzzer",
            f"build/fuzz-coverage/tests/{require_target(name)['binary'].removesuffix('_fuzz')}_cov",
            "--corpus",
            f"tests/fuzz/corpus/{name}",
            "--output",
            f"tests/fuzz/coverage/{name}",
        ]
    )


def coverage_minimize(name: str) -> None:
    require_target(name)
    run(
        [
            sys.executable,
            "tests/fuzz/minimize_corpus.py",
            "--fuzzer",
            f"build/fuzz-coverage/tests/{require_target(name)['binary'].removesuffix('_fuzz')}_cov",
            "--corpus",
            f"tests/fuzz/corpus/{name}",
            "--profdata",
            f"tests/fuzz/coverage/{name}/merged.profdata",
            "--output",
            f"tests/fuzz/corpus_minimal/{name}",
        ]
    )


def coverage_report(name: str) -> None:
    require_target(name)
    html = ROOT / "tests" / "fuzz" / "coverage" / name / "html"
    html.mkdir(parents=True, exist_ok=True)
    run(
        [
            "llvm-cov",
            "show",
            str(
                ROOT
                / "build"
                / "fuzz-coverage"
                / "tests"
                / f"{require_target(name)['binary'].removesuffix('_fuzz')}_cov"
            ),
            "--instr-profile",
            f"tests/fuzz/coverage/{name}/merged.profdata",
            "--ignore-filename-regex=_deps/",
            "--format=html",
            f"--output-dir={html}",
        ]
    )
    print(f"Report: tests/fuzz/coverage/{name}/html/index.html")


def coverage_all() -> None:
    for entry in targets():
        name = entry["name"]
        print(f"\n==> {name} coverage...\n────────────────────────────────────────", flush=True)
        coverage_collect(name)
        coverage_minimize(name)
    print("\n════════════════════════════════════════\nCoverage minimization complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)

    fuzz_parser = subparsers.add_parser("run", help="run one or all fuzz targets")
    fuzz_parser.add_argument("seconds", type=int)
    fuzz_parser.add_argument("target", help="target name or 'all'")
    fuzz_parser.add_argument("extra", nargs=argparse.REMAINDER, help="extra libFuzzer arguments")

    check_parser = subparsers.add_parser("check-target", help="validate a target name")
    check_parser.add_argument("target")
    subparsers.add_parser("coverage-all", help="collect and minimize every target's coverage")
    for action in ("merge", "coverage-collect", "coverage-minimize", "coverage-report"):
        action_parser = subparsers.add_parser(action, help=f"{action} for one target")
        action_parser.add_argument("target")

    args = parser.parse_args()
    if args.action == "run":
        extra = args.extra[1:] if args.extra[:1] == ["--"] else args.extra
        run_fuzzers(str(args.seconds), args.target, extra)
    elif args.action == "check-target":
        if not validate_target(args.target):
            raise SystemExit(2)
    elif args.action == "merge":
        merge_corpus(args.target)
    elif args.action == "coverage-collect":
        coverage_collect(args.target)
    elif args.action == "coverage-minimize":
        coverage_minimize(args.target)
    elif args.action == "coverage-report":
        coverage_report(args.target)
    else:
        coverage_all()


if __name__ == "__main__":
    main()
