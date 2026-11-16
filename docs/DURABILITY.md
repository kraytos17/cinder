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

Hard removal is persisted after the in-memory entry is erased. The current
`Store.Remove` API exposes only a boolean and its persistence hook ignores
append errors, so callers cannot observe a hard-remove WAL failure; the entry
may be absent in memory but reappear after recovery. Normal client `DEL` uses a
versioned tombstone path instead; hard removal is used for internal cleanup,
including completed migration.

`Manager.Sync` explicitly flushes the active WAL. Graceful `Manager.Close`
syncs and closes it, so a graceful daemon shutdown attempts to flush all WAL
appends that completed before the close. `Server.Kill` is a crash simulation:
it closes listeners/connections but deliberately does not close the persistence
manager and does not call `Sync`.

## Snapshots and compaction

Automatic compaction is a replay/recovery optimization, not a per-write
durability policy. Snapshot publication fsyncs the temporary snapshot before
rename, then publishes `CURRENT` with file and directory fsyncs. Compaction
publishes the snapshot before rotating the WAL; it then syncs and closes the
old active WAL before opening a fresh one or deleting sealed segments. If the
old WAL sync or close fails, compaction returns an error and does not rotate or
delete WAL segments. The already-published snapshot remains available, and the
old WAL remains attached to the manager so later writes do not silently bypass
persistence. A retry may publish another snapshot and retry rotation.

This ordering protects against losing recoverable data on a reported sync or
close failure: the synced snapshot contains the mutations covered by the
persistence barrier, while the old WAL remains intact for recovery. It does
not prove physical-device/power-loss behavior on every filesystem or storage
controller; see the recovery section and follow-up list below.

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
| Snapshot/compaction | Snapshot and `CURRENT` publication are synced before rotation; WAL sync/close failures abort rotation and preserve the old WAL. Hardware power-loss ordering still depends on filesystem/device guarantees and is not emulated by unit tests. |

## Counter state

When persistence is enabled, counter mutations append full CRDT state (or a
local remove marker) using WAL v3 record kinds. Recovery merges full state
component-wise, so WAL replay is idempotent and cannot lower a component.
Snapshots use the v4 container with the existing KV rows plus a counter bundle;
v1/v2 KV-only snapshots remain readable, while the older compressed v3
snapshot format remains unsupported. The WAL reader accepts legacy, v1, v2,
and v3 files; an old WAL header is upgraded before v3 records are appended.

Counter actor IDs are node IDs and must be stable across restarts. `cinderd`
requires an explicitly supplied `--node-id` whenever persistence or peers are
configured; do not reuse one node ID for two live processes. Counter state is
included in byte-based compaction pressure in addition to active WAL bytes.
Counter `Remove` is a local-only operation (not a replicated delete); a
persisted local removal is replayed locally, but a later peer merge can still
resurrect the counter as documented by `internal/counter`.

Hinted handoff uses `<data-dir>/hints.log` only when both persistence and
replication (`--replication-factor > 1`) are configured. With persistence off,
the bounded in-memory queue retries while the process lives, but queued hints
are lost on process crash. Journal recovery drops both hints whose journal
retry TTL elapsed and hints whose underlying entry TTL elapsed while the
target was unavailable.

## Follow-up before promising stronger durability

1. Make snapshot publication durable: fsync the temporary snapshot, rename it,
   fsync the directory, publish `CURRENT` atomically and durably, and only then
   discard older WAL files.
2. Propagate hard-remove persistence failures (the current `Remove` API only
   returns a boolean, and the hook error is ignored).
3. Add process-crash tests for policy behavior and power-loss-oriented tests
   using injectable file/sync operations; `SIGKILL` alone cannot prove that
   unsynced page-cache data was lost.
