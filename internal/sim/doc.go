// Package sim is a deterministic multi-node simulation harness for
// convergence and fault-injection tests. One Cluster wires the real
// subsystem managers (repl, ae, shard) per node over a shared virtual
// network; tests drive membership, faults, and the event pump explicitly,
// so every run is reproducible from its seed.
//
// What it is (and is not):
//
//   - Manager-level nodes: each Node holds the same managers a Server
//     wires (repl, ae, shard) plus its store, ring, and membership table.
//     There is no Server, no TCP, no background goroutines, no disk I/O.
//   - Single virtual clock: one store.ManualClock feeds every store,
//     manager, and table; the network speaks in durations since the
//     cluster base time. Time moves only when the pump advances it to a
//     queued deliver_at, or when the test calls Advance. No wall-clock
//     reads influence outcomes (ae's rehash backstop defaults still apply
//     but tests control the clock).
//   - Test-driven membership: Join/Leave arrive as MarkDead/MarkAlive on
//     every node's table (converged gossip), followed by a ring rebuild
//     that mirrors Server.rebuildRingFromTable, including the "always
//     append self" rule so a lone node owns everything.
//   - Faithful receive path: deliver routes each opcode to the same
//     manager entry point the TCP dispatch calls (applyReplicated,
//     GetVersioned, AE digest/sync/refinement, counter merge/AE).
//     The coordinator facade (Write/Delete/Get) mirrors the ownership
//     gate and replication branching of handleSet/handleDelete and the
//     replicated handleGet path, including the "moved to <owner>"
//     redirect shape.
//
// Determinism contract:
//
//   - With loss, jitter, and reorder disabled, identical scripts produce
//     identical event logs up to arrival order. The acceptance test
//     asserts byte-identical SortedEvents across two fresh clusters.
//   - Faults (down, partition, loss) make outcomes order-dependent by
//     design; tests that enable them assert converged state, never log
//     bytes. Jitter and loss draw from the shared seeded RNG; reorder is
//     order-only (adjacent swap of the due list), so sorted-log
//     comparison is unaffected by it.
//
// Deadlock policy: the pump fails loudly when parked senders remain with
// an empty queue (quorum short of votes under faults). Tests asserting
// unavailability expect the quorum error from the operation, which the
// pump surfaces by letting the operation observe the closed state —
// operations never hang; use short RF=2 clusters for partition tests.
package sim
