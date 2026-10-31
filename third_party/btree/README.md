# Vendored third-party dependency

## Kronuz B-tree v2026.10.22 (Apache-2.0)

In-memory B-tree ordered containers (`btree::map`, `btree::set`),
a modernized C++17 fork of Google's C++ B-tree. Header-only, no
dependencies. Multiple keys per node: fewer cache misses and less
memory overhead than node-per-element red-black trees.

- Upstream: https://github.com/Kronuz/btree
- Files: `btree/btree.h`, `btree/map.h` (only the map half is vendored;
  `set.h` was omitted — add it if a use appears).
- License: Apache-2.0 (see upstream `LICENSE`); upstream `NOTICE`
  attributes Copyright (c) 2013 Google Inc. and Copyright (c) 2019-2026
  Germán Méndez Bravo.
- Used by: `LfuStore` frequency buckets (`include/cinder/store/lfu_store.hpp`,
  ordered by frequency so the eviction minimum comes from `begin()`).
- Iterator/reference stability: unlike `std::map`, ANY insert or erase
  invalidates all iterators into the container. Callers must not hold a
  `FreqMap` iterator, `begin()` result, or mapped reference across a
  mutating call (`operator[]`, `insert`, `erase`, `clear`).

To update: download the two headers from upstream master, replace these
files, and record the version below. Re-verify the invalidation audit in
`src/store/lfu_store.cpp` (`evictOne`, `incrementFreq`,
`removeFromFreqBucket` hold no iterators across mutating calls).

Version: 25f9494 (master, no tags upstream)
Vendored: 2026-10-31
