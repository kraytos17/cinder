# Deterministic simulation (`internal/sim`)

Multi-node convergence and fault-injection tests without TCP, disk (except
persistence tests), or wall-clock. Each `sim.Node` wires the real
subsystem managers (`repl`, `ae`, `shard`) over a shared virtual network;
tests drive membership, faults, and the event pump explicitly, so every run
is reproducible from its seed.

## Contract

- **Manager-level nodes.** No `Server`, no background goroutines, no real
  I/O. Every clock reads one shared `ManualClock`; the network speaks in
  durations since the cluster base time. Time moves only when the pump
  advances it to a queued `deliver_at`, or when the test calls `Advance`.
- **Test-driven membership.** `MarkDead`/`MarkAlive` apply converged gossip
  to every node's table and rebuild rings (mirroring
  `Server.rebuildRingFromTable`, including "always append self").
  `Suspend`/`Resume` cut traffic without touching membership (crashed but
  not yet convicted: the peer stays a replica target, so failed sends
  become hints). `MarkDeadOn` convicts on one node only, for split-brain
  setups where both sides self-own.
- **Faithful receive path.** `deliver` routes each opcode to the same
  manager entry point the TCP dispatch calls; the coordinator facade
  (`Write`/`Delete`/`Get`, `CounterAdjust`/`CounterGet`) mirrors the
  ownership gate, replication branching, and redirect shape (`moved to
  <owner>`) of the handlers, including `ErrNotFound → NotFound` on quorum
  reads and capacity → `StatusCapacityExceeded` on writes.
- **Pump to quiescence.** Blocking operations (quorum, AE rounds) run in a
  goroutine while the caller pumps. `Pump` requires quiescence to persist
  across consecutive observations: quorum fan-out returns after W acks
  while a straggler send may not have queued yet.
- **Persistence.** `EnablePersistence` wires WAL + snapshots + counter
  persistence + the hint journal per node (mirroring
  `Server.ConfigurePersistence`, `FsyncAlways` for crash-safety);
  `RestartNode` drops all in-memory state, recovers from disk, rewires
  managers, and rejoins with a fresh incarnation.

## Determinism rules

With loss, jitter, and reorder disabled, identical scripts produce
identical event logs up to arrival order (`SortedEvents` removes it;
`TestSimDeterministicLogs` asserts byte identity). Faults make outcomes
order-dependent by design: tests that enable them assert converged state,
never log bytes. Jitter and loss draw from the shared seeded RNG, so the
seed is part of the fixture — a test that stops exercising its fault
(e.g. no drops recorded) must fail loudly on seed rot.

## Fault model

| Fault | API | Recovers via |
|---|---|---|
| Crash (convicted) | `MarkDead` / `MarkAlive` | ring rebuild, rebalance, AE |
| Crash (unconvicted) | `Suspend` / `Resume` | hint replay, AE |
| Process restart | `RestartNode` | disk recovery, rejoin |
| Partition | `Partition` / `HealAll` | heal, retry, AE |
| Loss | `SetLoss` | hints, AE |
| Delay spread | `SetDelay` / `SetJitter` | pump advances virtual time |
| Reordering | `SetReorder` | LWW idempotence |

## Adding a test

1. Build a cluster: `sim.NewCluster(t, ids, sim.Options{RF, Mode, Seed})`.
2. Find owners with the `ownerOf` helper (probe write + delete); find the
   peer with `peerOf` (two-node clusters).
3. Drive ops (`Write`/`Delete`/`Get`/`CounterAdjust`/`RunAE`/`Rebalance`/
   `ReplayHints`); each pumps internally.
4. Assert converged state on every node. For log identity, compare
   `SortedEvents` of two fresh clusters with faults disabled.
5. Rendezvous with background fan-out via `WaitForHint` before replaying;
   never sleep — spin on pump-observable state.
