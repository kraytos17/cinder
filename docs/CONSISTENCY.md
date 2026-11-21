# Consistency model

Cinder is a primary-routed, per-key last-write-wins (LWW) replicated cache. It
provides eventual convergence when replicas can communicate and repair runs;
it does **not** provide linearizability, serializable transactions, or a
globally ordered history of client operations. The `quorum` write mode narrows
the window in which a successful write exists on only one copy, but does not
change LWW into a consensus protocol.

## Version and conflict ordering

Each store starts its version counter from its local wall clock in Unix
nanoseconds. A local mutation calls `MintVersion`, which advances that
store-local counter. When a versioned entry is applied, the receiver advances
its next local version to at least the received version plus one. This is a
Lamport-style counter with a wall-clock initial value; it is not a hybrid
logical clock and does not measure or correct clock skew.

Replicas compare entries lexicographically by:

1. `version` (higher wins);
2. `writer_node_hash` (higher wins when versions tie); and
3. tombstone status (a tombstone wins an otherwise exact tie).

`writer_node_hash` is FNV-1a of the stable node ID. The same comparison is
used by store apply, quorum-read reduction, read repair, hints, and
anti-entropy. See `internal/store/lww.go` and `internal/repl/repl.go`.

Consequences:

- Concurrent writes can resolve to either writer according to their stamps;
  the winner is deterministic, but it need not be the write that completed
  last in real time.
- A node with a lagging wall clock can mint a lower initial version than a
  write already accepted elsewhere. If it has not observed that write, its
  later real-time operation can lose under LWW. A receiver that does observe a
  version advances its own counter, but this does not establish a global
  real-time order across every node.
- A failed quorum write may already have committed locally and may later
  propagate. A client error means the required acknowledgements were not
  obtained; it does not mean the attempted value was rolled back.
- A successful quorum write is an in-memory replication acknowledgement, not
  by itself a promise that the copies were fsynced. Disk durability depends on
  persistence being enabled and its configured fsync policy; see
  [`DURABILITY.md`](DURABILITY.md).

## Write modes

### `async`

The primary applies the write locally and returns success after that local
commit. It fans out best-effort to the other owners. Failed sends are queued as
bounded hints when possible; anti-entropy is another repair path. A successful
async response does not imply that any remote replica has applied the value.

### `quorum`

For a replica set of size `N` (the replication factor, including the primary),
the primary requires `W = floor(N/2) + 1` successful applies including its
local apply. The write is sent to the other ring-selected owners concurrently.
If `W` acknowledgements arrive, the request succeeds without waiting for every
remaining send. If a majority cannot be reached, the request fails, although
the local apply and any successful remote applies remain in place and may be
repaired or observed later.

The server does not expose a separately configurable read quorum `R`. Its
replicated read path sends versioned reads to the other selected owners and
uses the local copy as a vote when present. It stops once it has `N` positive
versioned responses or all remote responses have drained. Missing-key replies
and transport errors do not count as positive versioned votes. It is
availability-first: when responses drain before `N` positive votes, it may
still return the best LWW entry it saw. If it observed a missing or stale copy,
it flags repair and attempts best-effort read repair; a transport failure alone
does not guarantee a repair attempt. Thus this read path must not be described as a strict majority
read or as a guarantee that every successful write is returned during a
concurrent failure or membership change.

Even when all expected copies answer, quorum reads select the greatest stamp
among those responses; they do not serialize concurrent operations or prove
real-time order. Membership changes can also change the ring-selected replica
set between a write and a later read. See `internal/repl/write.go` and
`internal/repl/read.go` for the decision paths.

## Deletes and expiry

Deletes are versioned tombstones, not immediate erasures. A tombstone wins over
a lower stamp and over a live entry with the exact same version and writer.
After the configured tombstone-retention interval, garbage collection removes
the marker; a replica that returns with older data after that point can
reintroduce it unless the node is fenced and bootstrapped safely. Follow
[`OPERATIONS.md`](OPERATIONS.md) for the stale-replica procedure and its
limitations.

TTL is carried as an absolute expiry time through replication and repair.
Expiry is checked locally; an expired entry is not made live merely because it
arrives through repair or a hint. For end-to-end guarantees and remaining
cross-path test gaps, see `docs/INVARIANTS.md`.

## Evidence and limits

- Stamp order and stale-write rejection: `internal/store/versioned_test.go`
  (`TestVersionedNewerWins`, `TestVersionedStaleIgnored`,
  `TestVersionedTiebreakWriterHash`, `TestVersionedTombstoneWinsTies`).
- Quorum acknowledgement and read-repair behavior:
  `internal/repl/repl_test.go` (`TestWriteQuorum`,
  `TestReadQuorumLWWAndRepair`) and `internal/sim/sim_property_test.go`
  (`TestSimQuorumAckSurvivesOneReplicaOutage`).
- Acknowledged values across seeded membership churn and rebalance:
  `internal/sim/sim_property_test.go`
  (`TestSimRebalanceChurnPreservesAckedValues`). This covers 50 deterministic
  seeds; it is not an exhaustive proof for arbitrary schedules.
- Partition healing and LWW convergence:
  `internal/sim/sim_acceptance_test.go`
  (`TestSimPartitionHealConverges`, `TestSimAntiEntropyConverges`).
- Tombstone retention and over-age anti-entropy fence:
  `internal/net/tcp/tombstone_resurrection_test.go` and
  `docs/OPERATIONS.md`.

These tests demonstrate selected behaviors, not a formal consistency proof.
In particular, no test establishes linearizability, exact wall-clock ordering,
or safe recovery from every clock-skew and membership-churn schedule.
