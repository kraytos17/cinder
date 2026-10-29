# Vendored third-party dependency

## ankerl::unordered_dense v5.3.3 (MIT)

Fast, densely-stored `unordered_map`/`unordered_set` replacement
(header-only, no dependencies).

- Upstream: https://github.com/martinus/unordered_dense
- Files: `ankerl/unordered_dense.h`, `ankerl/stl.h` (both required; `stl.h`
  holds only standard includes, split out for `import std` builds).
- Used by: all hash maps/sets repo-wide (`include/`, `src/`, `tests/`,
  `benchmarks/`, `tools/`). `std::unordered_*` is not used anywhere.

To update: download the two headers from the upstream release tag,
replace these files, and record the version below.

Version: 5.3.3
Vendored: 2026-10-27
