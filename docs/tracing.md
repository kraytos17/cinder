# Tracing

Cinder propagates trace context on TCP frames as correlation metadata. It does
not create tracing spans or provide subscribers/exporters. The TCP server uses
the IDs in structured request and error logs; they are not a tracing substrate.
What exists:

- **TCP frames** carry `trace_id` / `span_id` (`uint64` each, appended when
  the trace flag is set). The TCP server generates IDs for missing/zero
  values, echoes them in the response, includes them in structured request
  logs, and propagates them to replicas on `REPLICATE`/`HINT`. They correlate
  logs but do not identify spans.
- **gRPC** ignores trace request metadata, including `cinder-trace-id` and
  `cinder-span-id`; gateway calls do not currently join a client-supplied
  trace context.

Possible conventions for a future tracing-substrate design (not current
behavior or a compatibility promise):

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
