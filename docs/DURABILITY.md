# Durability semantics

This document describes the current Go implementation. It distinguishes a
successful protocol response from data being stable on physical storage; those
are not equivalent under the current default policy.

## Acknowledgement and WAL behavior

With `--enable-persistence`, an accepted local KV mutation invokes the store's
persistence hook while holding the affected shard lock. The `--fsync` policy
controls when the WAL is synced:

* `none` (default) does not sync per write. `OK` means the append succeeded,
  not that the record is stable on disk.
* `always` syncs after each WAL append and before returning success. An append
  or sync error is returned as a failed write response; the already-applied
  in-memory value is not rolled back.
* `interval:<ms>` acknowledges successful appends without waiting for sync,
  while a background loop syncs the WAL at that interval, including while
  idle. A crash may lose recent appends from the interval window. Shutdown
  also syncs the WAL.

Hard-removal persistence errors cannot currently be returned because
`Store.Remove` exposes only a boolean result.

`Manager.Sync` explicitly flushes the active WAL. Graceful `Manager.Close`
syncs and closes it, so a graceful daemon shutdown attempts to flush all WAL
appends that completed before the close. `Server.Kill` is a crash simulation:
it closes listeners/connections but deliberately does not close the persistence
manager and does not call `Sync`.

## Snapshots and compaction

Automatic compaction is a replay/recovery optimization, not a per-write
durability policy. The current snapshot writer writes a temporary snapshot and
renames it into place, but does not fsync the snapshot, the `CURRENT` pointer,
or their containing directory. Compaction syncs/closes the WAL before opening
a fresh active WAL and pruning old files. Consequently, the current compaction
publication sequence is not proven power-loss-safe: an unclean system failure
around snapshot publication/rotation may leave neither a durable snapshot nor
the old WAL records. Compaction also currently ignores errors returned by the
WAL `Sync` and `Close` calls before rotation, so it can proceed to replace the
active log after a failed sync. Do not treat successful compaction as a stronger
durability guarantee until the file and directory fsync sequence is fixed,
errors are propagated, and the sequence is tested.

## Recovery and torn tails

Recovery loads the latest available snapshot, then replays sealed WAL segments
and the active WAL in order. A record that ends before it is completely
decoded is treated as a torn tail. Recovery keeps the last complete record,
truncates the file to `Reader.ValidSize()`, and resumes appending there. A
checksum mismatch or structurally invalid record is corruption, not a torn
tail, and recovery must fail instead of silently truncating valid-looking
prefix data.

The format-level distinction is covered by `internal/persist/wal_test.go`:
`TestWALTornTail` and `TestWALCorruptionDetected`. Recovery-level truncation
and refusal of corrupt records are covered by
`internal/persist/manager_test.go`. These tests exercise file images and
ordinary process behavior; they do not emulate loss of the
kernel page cache or physical-device write reordering.

Startup uses a fail-closed corruption policy: a checksum mismatch, malformed
record, unsupported WAL header, or invalid snapshot aborts recovery. The only
automatic repair is truncation of an incomplete final WAL record to the last
valid record boundary. Mid-file repair/quarantine is deliberately not
implemented; preserve a copy of the data directory and investigate before
attempting manual repair.

`cinderd --verify-data-dir <dir>` runs the offline read-only verifier. It
reports recognized snapshot/WAL files, formats, KV/counter record counts,
valid WAL bytes, corruption file offsets, and recoverable torn tails. It exits
1 for corruption or unsupported formats; torn tails are warnings and do not
modify the directory. It can be used before restart or against a copied
directory while the daemon remains stopped. For example:

```sh
cinderd --verify-data-dir /var/lib/cinder
```

Verification is diagnostic, not repair. Normal startup still truncates a
torn WAL tail to its valid prefix; the verifier never does.

## Current guarantee summary

| Event / response | Current guarantee |
|---|---|
| Successful write response with `--fsync=none` | Mutation was accepted in memory and the WAL append succeeded; no per-write fsync occurs. |
| Successful write response with `--fsync=always` | Mutation was accepted, WAL append succeeded, and WAL `Sync` returned successfully before the response. |
| Successful write response with `--fsync=interval:<ms>` | Mutation was accepted and appended; periodic WAL sync bounds the nominal loss window by the configured interval while the process is healthy. |
| Graceful shutdown | Active WAL `Sync` is attempted during close; close/sync errors are logged or returned where the caller can observe them. |
| `Kill` / process crash | No fsync is performed by the simulated kill path. Unsynced records may or may not survive depending on the OS/filesystem. |
| Torn WAL tail | Complete prefix is replayed; incomplete suffix is truncated on recovery. |
| Checksum/structural corruption | Recovery should fail; never regard a checksum mismatch as a clean torn tail. |
| Snapshot/compaction | Atomic rename is used, but file and directory fsync are absent; power-loss durability is not guaranteed. |

## Counter state

When persistence is enabled, counter mutations append full CRDT state (or a
local remove marker) using WAL v3 record kinds. Recovery merges full state
component-wise, so WAL replay is idempotent and cannot lower a component.
Snapshots use the v4 container with the existing KV rows plus a counter bundle;
v1/v2 KV-only snapshots remain readable, while the older compressed v3
snapshot format remains unsupported. The WAL reader still accepts legacy, v1,
and v2 files; an old WAL header is upgraded before v3 records are appended.

Counter actor IDs are node IDs and must be stable across restarts. `cinderd`
requires an explicitly supplied `--node-id` whenever persistence or peers are
configured; do not reuse one node ID for two live processes. Counter state is
included in byte-based compaction pressure in addition to active WAL bytes.
Counter `Remove` is a local-only operation (not a replicated delete); a
persisted local removal is replayed locally, but a later peer merge can still
resurrect the counter as documented by `internal/counter`.

## Follow-up before promising stronger durability

1. Make snapshot publication durable: fsync the temporary snapshot, rename it,
   fsync the directory, publish `CURRENT` atomically and durably, and only then
   discard older WAL files.
2. Propagate hard-remove persistence failures (the current `Remove` API only
   returns a boolean).
3. Add process-crash tests for policy behavior and power-loss-oriented tests
   using injectable file/sync operations; `SIGKILL` alone cannot prove that
   unsynced page-cache data was lost.
