# Tracing

Cinder's tracing substrate (`cinder/common/tracing.hpp`) provides spans with
explicit cross-thread context, typed structured events, and a pluggable
subscriber backend. This note covers the conventions; the plan in
`.opencode/plan/cinder-comprehensive-tracing.md` tracks the multi-phase rollout.

## Operation naming

Stable dotted names, one per operation boundary:

- `client.*` — public client ops (`client.get`, `client.multi_set`, …).
- `tcp.*` — ingress handling (`tcp.request.get`, `tcp.sendResponse`, …).
- `replication.*`, `transport.*` / `pool.*`, `cluster.*` (`gossip.tick`,
  `membership.apply_rumor`, `failure.probe`), `shard.*`, `anti_entropy.*`,
  `persistence.*`, `config.*`, `grpc.*`.

## Spans

- A span is created per operation; nested spans use the live parent, remote
  parents (wire requests, async callbacks on other executors) use the explicit
  `SpanContext` overload — never infer remote trace identity from ambient
  thread-local state.
- `setStatus(Ok|Error|Cancelled|Timeout)` records the outcome; last call wins,
  destruction always closes exactly once. Unset means "never reported".
- `addEvent(name)` records a timestamped milestone (phase transitions, async
  hop boundaries). Prefer `CINDER_*` events for hot paths.
- Spans only manage context without a subscriber; lifecycle, status and events
  are delivered to the installed subscriber when one was present at
  construction.

## Client propagation

Public client operations (`client.*`) create a root span and stamp its
context onto every outbound request (`client.route` covers a key's
redirect/retry chain as one child unit), so the server joins the client trace
instead of minting a fresh root. Batch operations stamp the batch span's
context on all sub-requests.

Span status convention: `Ok` when the operation was served (including cache
misses and ownership redirects — those are well-formed answers), `Error` on
transport failure or store/replication failure. Ambiguous outcomes (e.g. a TTL
lookup that returns empty on miss, timeless key or failure) stay `Unset`; the
child span holds the detail.

## Async propagation

Quorum fan-out captures the submitting thread's trace identity; completions
draining inline already run under it, while the async tail (system-executor
threads with unrelated TLS) re-attaches it around every terminal callback —
never across a suspension. Per-replica child spans
(`replication.write.replica`, `replication.read.replica`) parent to the
fanout span; quorum decisions and hint-replay outcomes record `Ok`/`Error`.
Transport and pool exchanges join the request's trace when it carries one
(fresh root for background traffic) and report
`Ok`/`Error`/`Timeout`/`Cancelled` outcomes.

## Recording

`RecordingSubscriber` captures complete span records (ids, parentage,
operation, monotonic start/end, status, events) plus correlated log events.
It owns copies of all strings and swallows its own faults. Use it in tests and
as the base for future exporters — there is no network exporter by default.

## Inbound trace metadata (gRPC)

Callers propagate a trace parent with two request metadata entries:

- `cinder-trace-id`: ASCII decimal uint64, nonzero
- `cinder-span-id`: ASCII decimal uint64, nonzero

Only the first value of each key is honored. Absent or malformed values are
ignored and the RPC starts a fresh local trace — a bad trace parent never
fails the request. Keys are lowercase per gRPC convention; values are parsed
strictly (no whitespace, signs, hex, or >20 digits) so a truncated or binary
value can never alias another trace. Metadata contents are never logged.

TCP needs no equivalent: the v5 frame already carries `trace_id`/`span_id`.

## Threading

Subscriber callbacks are `noexcept` and must stay cheap. `LayeredSubscriber`
dispatches without holding its composition lock: layers must be thread-safe,
but reentrant use (emitting events from inside a callback) is safe.

## Background work and membership

Timer callbacks run on I/O threads with unrelated ambient context, so each
scheduled run starts a fresh trace root (`timer.evict`, gossip ticks, failure
probes, anti-entropy rounds, shard rebalances). The node startup/shutdown
sequences are likewise fresh roots (`node.start`, `node.shutdown`).

Work *caused* by another trace joins it instead: `rebuildRing` runs
synchronously inside the membership mutation that triggered it, gossip/probe
handlers run under the ingress span, and async completions re-attach the
captured parent with `TraceScope` at callback entry (never across a
`co_await`, which may resume on another thread). Timer callbacks that fire
follow-up RPCs (gossip, probes, anti-entropy, hint replay) stamp the outgoing
request with the run's context so the peer side joins the same trace.

## Storage

`persistence.recover/compact/replay/flush/shutdown`, `snapshot.write/read`,
and `hint.journal.open/compact` report `Ok`/`Error` outcomes. WAL appends
happen only in batched drains covered by the flush span; per-hint journal
appends ride the calling span instead of minting their own.

## Privacy

Never record values, keys, secrets, bearer material, or TLS state. Node IDs,
opcodes, counts, bytes and durations are operator-visible and fine.
