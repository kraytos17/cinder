"""Unit tests for the developer workflow scripts (stdlib only, no build needed).

Run with: python3 scripts/tests/test_workflows.py  (or: just test-scripts)
"""

from __future__ import annotations

import contextlib
import io
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
ROOT = SCRIPTS_DIR.parents[0]
sys.path.insert(0, str(SCRIPTS_DIR))

import check_preset  # noqa: E402
import check_tools  # noqa: E402
import deps  # noqa: E402
import doctor  # noqa: E402
import fuzz  # noqa: E402
import pgo  # noqa: E402
import presets  # noqa: E402
import run_gtests  # noqa: E402


class TestFuzzRegistry(unittest.TestCase):
    def test_registry_loads_with_unique_valid_names(self) -> None:
        entries = fuzz.targets()
        self.assertTrue(entries)
        names = [entry["name"] for entry in entries]
        self.assertEqual(len(names), len(set(names)))
        for entry in entries:
            self.assertRegex(entry["name"], r"^[a-z0-9_]+$")
            self.assertIsInstance(entry["options"], list)

    def test_known_targets_present(self) -> None:
        names = {entry["name"] for entry in fuzz.targets()}
        for expected in ("ring", "wal", "store_put", "protocol_decode"):
            self.assertIn(expected, names)

    def test_validate_target(self) -> None:
        self.assertTrue(fuzz.validate_target("ring"))
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertFalse(fuzz.validate_target("bogus-target-xyz"))

    def test_require_target(self) -> None:
        self.assertEqual(fuzz.require_target("ring")["name"], "ring")
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as ctx:
                fuzz.require_target("bogus-target-xyz")
        self.assertEqual(ctx.exception.code, 2)


class TestFuzzRun(unittest.TestCase):
    def setUp(self) -> None:
        patcher = mock.patch.object(fuzz.subprocess, "run")
        self.mock_run = patcher.start()
        self.addCleanup(patcher.stop)
        seed_patcher = mock.patch.object(fuzz, "seed_corpus")
        seed_patcher.start()
        self.addCleanup(seed_patcher.stop)

    def test_single_target_builds_expected_command(self) -> None:
        with contextlib.redirect_stdout(io.StringIO()):
            fuzz.run_fuzzers("10", "ring", ["-runs=1"])
        self.mock_run.assert_called_once()
        command = self.mock_run.call_args.args[0]
        self.assertEqual(
            command,
            [
                str(ROOT / "build" / "fuzz" / "tests" / "ring_fuzz"),
                "-max_total_time=10",
                "-print_final_stats=1",
                "-timeout=5",
                "-max_len=1024",
                str(ROOT / "tests" / "fuzz" / "corpus" / "ring"),
                "-runs=1",
            ],
        )

    def test_run_all_covers_every_target(self) -> None:
        with contextlib.redirect_stdout(io.StringIO()):
            fuzz.run_fuzzers("10", "all", [])
        self.assertEqual(self.mock_run.call_count, len(fuzz.targets()))

    def test_unknown_target_runs_nothing(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as ctx:
                fuzz.run_fuzzers("10", "bogus-target-xyz", [])
        self.assertEqual(ctx.exception.code, 2)
        self.mock_run.assert_not_called()

    def test_missing_dict_is_dropped(self) -> None:
        entries = [{"name": "ring", "options": ["-max_len=64", "-dict=/nonexistent/x.dict"]}]
        with mock.patch.object(fuzz, "targets", return_value=entries):
            with contextlib.redirect_stdout(io.StringIO()):
                fuzz.run_fuzzers("1", "ring", [])
        command = self.mock_run.call_args.args[0]
        self.assertIn("-max_len=64", command)
        self.assertFalse(any(part.startswith("-dict=") for part in command))

    def test_merge_builds_expected_command(self) -> None:
        with mock.patch.object(Path, "mkdir"):
            fuzz.merge_corpus("ring")
        command = self.mock_run.call_args.args[0]
        self.assertEqual(
            command,
            [
                str(ROOT / "build" / "fuzz" / "tests" / "ring_fuzz"),
                "-merge=1",
                str(ROOT / "tests" / "fuzz" / "corpus_minimal" / "ring"),
                str(ROOT / "tests" / "fuzz" / "corpus" / "ring"),
            ],
        )

    def test_merge_unknown_target_runs_nothing(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                fuzz.merge_corpus("bogus-target-xyz")
        self.mock_run.assert_not_called()

    def test_coverage_commands_reference_target(self) -> None:
        for action in (
            fuzz.coverage_collect,
            fuzz.coverage_minimize,
        ):
            with self.subTest(action=action.__name__):
                self.mock_run.reset_mock()
                action("ring")
                command = self.mock_run.call_args.args[0]
                self.assertTrue(any("ring" in str(part) for part in command))

    def test_coverage_report_unknown_target_runs_nothing(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()), mock.patch.object(Path, "mkdir"):
            with self.assertRaises(SystemExit):
                fuzz.coverage_report("bogus-target-xyz")
        self.mock_run.assert_not_called()


class TestPresets(unittest.TestCase):
    def setUp(self) -> None:
        self.entries = presets.presets()
        self.root = presets.ROOT

    def test_shared_pgo_build_dir(self) -> None:
        self.assertEqual(
            presets.binary_directory("pgo-generate", self.entries), self.root / "build" / "pgo"
        )
        self.assertEqual(
            presets.binary_directory("pgo-use", self.entries), self.root / "build" / "pgo"
        )

    def test_plain_presets_map_to_build_name(self) -> None:
        self.assertEqual(
            presets.binary_directory("debug", self.entries), self.root / "build" / "debug"
        )
        self.assertEqual(
            presets.binary_directory("release-clang", self.entries),
            self.root / "build" / "release-clang",
        )

    def test_unknown_preset_raises(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                presets.binary_directory("bogus-preset-xyz", self.entries)

    def test_hidden_base_preset_is_not_listed(self) -> None:
        self.assertNotIn("base", self.entries)


class TestDepsGuards(unittest.TestCase):
    def test_clean_refuses_suspicious_paths(self) -> None:
        home = Path.home()
        for bad in (Path("/"), home, home / ".cache"):
            with self.subTest(path=str(bad)), mock.patch.object(
                deps, "cache_dir", return_value=bad
            ), mock.patch("sys.stderr"):
                with self.assertRaises(SystemExit):
                    deps.clean()

    def test_clean_missing_dir_is_a_noop(self) -> None:
        missing = Path("/nonexistent-cinder-test-cache-dir-xyz")
        with mock.patch.object(deps, "cache_dir", return_value=missing):
            with mock.patch("sys.stdout"):
                deps.clean()  # must not raise

    def test_empty_cmake_output_aborts(self) -> None:
        fake = mock.Mock(stdout="  \n")
        with mock.patch.object(deps.subprocess, "run", return_value=fake):
            with self.assertRaises(SystemExit):
                deps.cache_dir()


class TestRunGtests(unittest.TestCase):
    def test_groups_cover_expected_binaries(self) -> None:
        with (run_gtests.GROUPS_FILE).open(encoding="utf-8") as stream:
            groups = json.load(stream)
        for group in ("all", "grpc", "node"):
            self.assertTrue(groups[group])
        self.assertLess(set(groups["grpc"]), set(groups["all"]))
        self.assertLess(set(groups["node"]), set(groups["all"]))
        self.assertIn("cinder_unit_node_tests", groups["node"])

    def test_group_names_are_unique_within_group(self) -> None:
        with (run_gtests.GROUPS_FILE).open(encoding="utf-8") as stream:
            groups = json.load(stream)
        for name, binaries in groups.items():
            with self.subTest(group=name):
                self.assertEqual(len(binaries), len(set(binaries)))


class TestCheckHelpers(unittest.TestCase):
    def test_check_preset_accepts_known(self) -> None:
        with mock.patch.object(check_preset.sys, "argv", ["check_preset", "configure", "debug"]):
            check_preset.main()
        with mock.patch.object(check_preset.sys, "argv", ["check_preset", "test", "debug"]):
            check_preset.main()

    def test_check_preset_rejects_unknown(self) -> None:
        with mock.patch.object(
            check_preset.sys, "argv", ["check_preset", "configure", "bogus-preset-xyz"]
        ), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                check_preset.main()

    def test_check_tools(self) -> None:
        with mock.patch.object(check_tools.sys, "argv", ["check_tools", "python3"]):
            check_tools.main()
        with mock.patch.object(check_tools.sys, "argv", ["check_tools", "bogus-tool-xyz"]):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as ctx:
                check_tools.main()
        self.assertEqual(ctx.exception.code, 1)


class TestPgo(unittest.TestCase):
    def test_has_profiles_empty_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertFalse(pgo.has_profiles(Path(tmp)))

    def test_has_profiles_with_gcda(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp) / "build" / "pgo-data"
            data.mkdir(parents=True)
            (data / "foo.gcda").write_text("x")
            self.assertTrue(pgo.has_profiles(Path(tmp)))

    def test_check_aborts_without_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                pgo.check(Path(tmp))

    def test_version_comparison(self) -> None:
        self.assertTrue(doctor.version_tuple("4.4.5") >= (4, 4))
        self.assertTrue(doctor.version_tuple("4.3") < (4, 4))


if __name__ == "__main__":
    unittest.main(verbosity=2)
