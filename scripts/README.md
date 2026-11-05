# Developer workflow scripts

`Justfile` owns the public recipe names, preset selection, dependencies, and
argument passthrough. These Python helpers own the longer control flow so it is
testable/readable without turning the Justfile into a shell program.

## Customize the workflows

- **Fuzz targets:** edit `fuzz_targets.json` (one object per target, with the
  exact binary suffix and libFuzzer options). `fuzz.py` uses this registry for
  run-all, run-one validation, missing-dictionary handling, per-target
  `merge` / `coverage-collect` / `coverage-minimize` / `coverage-report`, and
  `coverage-all`. New subcommands go there so every entry point validates the
  target name the same way.
- **Direct GoogleTest suites:** edit `test_binaries.json`. Add a named group
  there, then expose it as a small recipe if users need a public entry point.
  `run_gtests.py` handles sequential execution, extra argv, and fail-fast
  reporting.
- **Presets:** `presets.py` reads `CMakePresets.json`; do not duplicate preset
  names or descriptions in the Justfile.
- **Tool requirements:** `doctor.py` is the source for required and optional
  developer tools. Keep Python 3 required because these scripts are invoked by
  Just.
- **PGO/data housekeeping:** `pgo.py` and `deps.py` keep validation and guarded
  filesystem operations out of recipes. Destructive public recipes retain
  Just's confirmation prompts.

Scripts are run as `python3 scripts/<name>.py`; executable bits/shebangs are
not needed. Run `python3 -m py_compile scripts/*.py`, `just --fmt --check`, and
`just --list` after changing the workflow.

## Tests

`scripts/tests/test_workflows.py` covers the scripts with stdlib `unittest`
only — registries load, names validate, commands are built correctly, guards
refuse dangerous paths. No build, no network, no repo mutation:

    python3 scripts/tests/test_workflows.py   # or: just test-scripts

`just test-scripts` is part of the `just verify` pre-push gate. Keep new
workflow logic covered the same way: pure helpers where possible, subprocess
and filesystem effects behind seams `unittest.mock` can intercept.
