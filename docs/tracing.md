# Tracing

Cinder propagates trace context on the wire but has no in-process tracing
substrate yet: there are no spans, no subscribers, no exporters, and no
overhead to budget. What exists:

- **TCP frames** carry `trace_id` / `span_id` (`uint64` each, appended when
  the flags say so). The server mints a `trace_id` when the client sends
  zero, echoes it in the response, and propagates it to replicas on
  `REPLICATE`/`HINT`. The values are correlation metadata only — nothing
  reads them locally.
- **gRPC** ignores request metadata, including the `cinder-trace-id` /
  `cinder-span-id` keys a tracing peer would send (lowercase, ASCII
  decimal, first value wins — the contract is reserved for when a
  substrate lands).

Reserved conventions for that landing (kept stable so a future
implementation does not need a wire change):

- One root span per operation; nested spans use the live parent, remote
  parents use the explicit context — never ambient state.
- Spans record outcome (`Ok` on served operations including misses and
  redirects, `Error` on transport/store/replication failure), low-cardinality
  attributes (opcode, phase, counts, bytes), and never keys, values,
  secrets, or bearer material.
- Client operations stamp their context onto every outbound request so the
  server joins the client trace instead of minting a fresh root.
- Timer callbacks (gossip, probes, anti-entropy, rebalance) start fresh
  roots; work caused by another trace joins it instead.

## Privacy

Never record values, keys, secrets, bearer material, or TLS state. Node IDs,
opcodes, counts, bytes and durations are operator-visible and fine.
