# Cinder invariants register

This register links system promises to current tests. **UNCOVERED** means no
test currently proves the full stated property; related unit tests are not
treated as proof of a distributed or crash-safety claim.

| ID | Invariant | Evidence / coverage |
|---|---|---|
| INV-LWW-1 | A higher `(version, writer)` stamp wins; merge is deterministic and a stale value cannot replace a newer value. | Covered locally: `internal/store/versioned_test.go` (`TestVersionedNewerWins`, `TestVersionedStaleIgnored`, `TestVersionedTiebreakWriterHash`). Cross-replica fault convergence is covered by selected simulations but randomized property coverage is **UNCOVERED**. |
| INV-TOMB-1 | A tombstone is not overwritten by a lower stamp and a stale delete does not erase a newer value. | Covered locally: `internal/store/versioned_test.go` (`TestVersionedStaleDeleteLoses`, `TestVersionedTombstoneWinsTies`). Partition/heal behavior beyond tombstone-GC grace is **UNCOVERED** (known resurrection risk; see G15). |
| INV-CRDT-1 | Counter merge is commutative, associative, and idempotent; components do not decrease when merging. | Partially covered: `internal/counter/counter_test.go` (`TestGCounterMergeTakesMax`, `TestPNCounterValueAndMerge`, `TestPNCounterConcurrentIncrementsSurvive`); AE has delivery-loss/reorder/duplicate coverage in `internal/ae/counter_test.go`. Exhaustive algebraic/property testing is **UNCOVERED**. |
| INV-REBAL-1 | Local data is removed only when the required new holders have acknowledged migration; failed migration retains the source copy. | Covered by focused tests: `internal/shard/shard_test.go` (`TestLeavingMigratesAndRemoves`, `TestMigrationFailureKeepsLocal`, `TestTombstoneAndTTLMigrate`, `TestSingleCopyMigrates`) and TCP rebalance tests. Random membership changes interleaved with writes are **UNCOVERED**. |
| INV-QUORUM-1 | A successful quorum write has acknowledgements from W replicas, and with `W+R>N` an eligible quorum read observes the acknowledged stamp. | Unit-level quorum behavior: `internal/repl/repl_test.go` (`TestWriteQuorum`, `TestReadQuorumLWWAndRepair`) and TCP integration coverage. Randomized crash/partition proof for every acknowledged write is **UNCOVERED**. |
| INV-WAL-1 | Recovery includes all writes durable under the configured acknowledgement/fsync policy and never silently accepts corrupt state. | Partial coverage: `internal/persist/wal_test.go` (`TestWALRoundTrip`, `TestWALTornTail`, `TestWALCorruptionDetected`), `internal/persist/manager_test.go` (`TestRecoverRoundTrip`, `TestRecoverTruncatesEveryTornTailOffset`, `TestRecoverRejectsCorruptWALRecord`), and TCP restart tests. Exact acknowledgement-to-fsync guarantee and randomized process-crash matrix are **UNCOVERED**. |
| INV-TTL-1 | An expired entry is not returned by reads and does not reappear through repair, hint replay, or anti-entropy. | Local expiry: `internal/store/versioned_test.go` (`TestVersionedTTLExpires`) and LRU tests. TTL is covered in read repair (`internal/repl/repl_sim_test.go`, `TestSimReadRepairPreservesTtl`) and hint expiry (`internal/repl/repl_test.go`, `TestHintTTLExpiry`). Cross-path clock-jump property testing is **UNCOVERED**. |

## Follow-up

Phase 4 should turn the uncovered distributed/crash cases into seeded
properties and add test identifiers here as they become proven. Do not mark an
invariant covered solely because a happy-path integration test exists.
