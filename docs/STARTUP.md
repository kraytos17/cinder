# Daemon startup ordering

`cinderd` configures node dependencies before its data listener serves
requests or cluster background loops begin:

1. Resolve the strict JSON `--config` file (if supplied), overlay explicitly
   passed flags, and validate the effective configuration; build the store and
   `tcp.Server`.
2. `ConfigureCluster` prepares the ring, membership table, peer pool, gossip
   manager, and failure detector. It seeds self's advertised address and
   incarnation, but does **not** start gossip or probes.
3. Configure rebalance and local server TLS.
4. Apply node/client secrets. `ConfigureTLS` and `ConfigureSecrets` also update
   the prepared gossip/failure-detector pool. The separate replication pool
   is created later with the same TLS and node-secret settings.
5. `ConfigurePersistence` recovers the WAL/snapshot into the store before it
   installs the mutation hook. A recovery error aborts startup without
   advertising the node.
6. Configure replication and, when enabled, recover/open the hint journal.
   The gossip/failure-detector pool uses its own bounded RPC timeout,
   configured independently from replication/AE's `--rpc-timeout-ms`.
7. Bind the data listener and enabled metrics/gRPC listeners. Start the edge
   listener servers, then call TCP `Serve`. `Serve` refreshes the advertised
   port from the actual listener and starts cluster activity before starting
   gossip/probe and other background loops.

This order prevents peers from learning a new node as alive before local
recovery finishes. Tests that configure a `tcp.Server` directly may rely on
`Serve` to start cluster activity; tests needing to observe the pre-start state can
inspect membership after `ConfigureCluster` and before `Serve`. SIGHUP
re-reads the JSON config file and applies its reloadable projection; it is not
a sparse patch and does not change settings that require restart.

## Verification

- `internal/net/tcp/persistence_test.go`
  `TestClusterDoesNotAdvertiseNodeBeforePersistenceRecovery` asserts the
  recovering node is absent from a peer's membership until startup is allowed
  to proceed.
- `internal/net/tcp/tls_test.go`
  `TestSecureThreeNodeClusterStartupConverges` configures TLS and shared node
  secrets after cluster preparation and verifies every node converges.
- `cmd/cinderd/addresses_test.go` covers automatic and explicit gossip RPC
  timeout resolution and validates that probe RPCs fit within detector timing.

Gossip carries the configured host and TCP port; TLS certificates are shared
with the pool through the existing `tlscfg` behavior. Address changes across
restarts are incarnation-guarded; see `docs/rebalance.md`.
