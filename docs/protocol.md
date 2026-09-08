# Cinder Wire Protocol v4

Binary, length-prefixed, request-response protocol over TCP. All multi-byte
integers on the wire are **big-endian** (network byte order). On-disk formats
(snapshot, WAL) use native byte order.

## Frame Header

Every message begins with a fixed 7-byte header. Its size is derived at
compile time via `consteval` and verified with `static_assert`.

| Offset | Size | Field         | Description |
|--------|------|---------------|--------------|
| 0      | 1    | `magic`       | `0xC1` — identifies a Cinder frame |
| 1      | 1    | `version`     | `0x04` |
| 2      | 1    | `opcode`      | Request: 1–16 (see [Opcodes](#opcodes)). Response: `0x00` |
| 3      | 4    | `payload_len` | Payload length in bytes, `uint32` |

**Limits and validation:**
- Max total message size: `K_MAX_MESSAGE_SIZE = 67,108,864` (64 MiB), enforced on
  both encode and decode. On decode, `payload_len` is checked against this
  bound before the body is read.
- `opcode` must fall in `1..16`; any other value is rejected as unknown. A
  `consteval` function, `opcodeRangeCoverage()`, asserts at compile time that
  this range is contiguous with no gaps.

## Request Payload

All request opcodes share one field layout. `ttl_ms` and `expires_at_ms` are
only present when their corresponding flag bit is set, so byte offsets shift
depending on which flags are set.

| Field              | Size | Present when     |
|--------------------|------|-------------------|
| `flags`             | 1    | always |
| `ttl_ms`            | 4    | `flags & 0x01` |
| `expires_at_ms`     | 8    | `flags & 0x02` |
| `trace_id`          | 8    | `flags & 0x04` |
| `span_id`           | 8    | `flags & 0x04` |
| `version`           | 8    | always |
| `writer_node_hash`  | 8    | always |
| `key_len`           | 4    | always |
| `key`               | N    | always (may be empty) |
| `val_len`           | 4    | always |
| `value`             | M    | always (may be empty) |

- **`flags`** — bit 0: `has_ttl`; bit 1: `has_expires_at`; bit 2: `has_trace`.
- **`ttl_ms`** (`uint32`) — relative TTL in milliseconds. Sent by clients on
  `SET`; the server converts it to an absolute expiry using its own clock.
- **`expires_at_ms`** (`uint64`) — absolute wall-clock expiry (Unix epoch ms).
  Sent by the primary on `REPLICATE`/`HINT` so every replica expires the key
  at the same instant regardless of delivery delay. Replicas convert it to
  their local steady-clock basis before storing.

  Overflow guard: values above `int64_t::max() / 1,000,000` (~9.2 × 10¹² ms,
  ≈ year 2262) are rejected. This prevents overflow when the value is later
  converted from `milliseconds` to a nanosecond-resolution `time_point`
  (a ×1,000,000 multiply).
- **`trace_id`** / **`span_id`** (`uint64` each) — distributed trace context
  for end-to-end request correlation. When a client sends `trace_id = 0`, the
  server generates one; the value is echoed back in the response and propagated
  to replicas on `REPLICATE`/`HINT`. A receiver that does not understand this
  flag ignores the trailing 16 bytes (backward-compatible: old nodes treat the
  extra bytes as part of the value field and reject via `InvalidArgument`,
  but forward-compatible nodes skip over unknown trailing data).
- **`version`** (`uint64`) — monotonic LWW version. Meaningful on
  `SET`/`REPLICATE`/`HINT`; `0` elsewhere.
- **`writer_node_hash`** (`uint64`) — stable per-node hash used to break LWW
  version ties. Meaningful on writes; `0` elsewhere.

`ttl_ms` and `expires_at_ms` are mutually exclusive in practice: clients send
the former on writes, replication uses the latter.

## Opcodes

| Op | Name | Field usage |
|----|------|-------------|
| 1 | `GET` | `key` set; no `ttl`/`expires_at`; `value` empty |
| 2 | `SET` | `key` + `value` set; `ttl_ms` set for a relative TTL; `version`/`writer_node_hash` optionally carry LWW metadata |
| 3 | `DEL` | `key` set |
| 4 | `PING` | all fields empty/zero |
| 5 | `GOSSIP` | membership view in `value` — `;`-delimited `id@host:port:state:incarnation` entries, e.g. `node1@127.0.0.1:7000:alive:3;node2@127.0.0.1:7001:dead:7`; `key` empty |
| 6 | `REPLICATE` | `key` + `value` set; `expires_at_ms` set when the primary computed an absolute expiry; `version` + `writer_node_hash` carry LWW metadata |
| 7 | `HINT` | same layout as `REPLICATE` |
| 8 | `GET_VERSIONED` | `key` set; response carries `version` + `writer_node_hash` for LWW comparison (quorum reads, read repair) |
| 9 | `ANTI_ENTROPY_DIGEST` | initiator sends its bucket digest (see [Anti-Entropy Payloads](#anti-entropy-payloads)); `key` empty |
| 10 | `ANTI_ENTROPY_SYNC` | initiator sends entries for divergent buckets; `key` empty |
| 11 | `ADMIN_INFO` | request: empty; response: `value` = JSON node info (node_id, port, capacity, store stats, config) |
| 12 | `ADMIN_CLUSTER` | request: empty; response: `value` = JSON membership array (id, host, port, state, incarnation) |
| 13 | `ADMIN_RING` | request: empty; response: `value` = JSON ring snapshot (self, vnodes_per_node) |
| 14 | `ADMIN_COMPACT` | request: empty; response: `status` = OK on success |
| 15 | `ADMIN_CONFIG_RELOAD` | request: empty; response: `status` = OK on success |
| 16 | `ADMIN_SHUTDOWN` | request: empty; response: `status` = OK, then server shuts down gracefully |

Admin opcodes (11-16) bypass ring ownership checks and are client-initiated only.

## Response Payload

The opcode byte on a response frame is always `0x00`.

| Field              | Size | Present when |
|--------------------|------|---------------|
| `status`            | 1    | always |
| `flags`             | 1    | always (bit 0: version metadata; bit 1: `expires_at`; bit 2: trace) |
| `version`           | 8    | `flags & 0x01` |
| `writer_node_hash`  | 8    | `flags & 0x01` |
| `expires_at`        | 8    | `flags & 0x02` |
| `trace_id`          | 8    | `flags & 0x04` |
| `span_id`           | 8    | `flags & 0x04` |
| `has_val`           | 4    | always |
| `value`             | M    | `has_val != 0` |

The `flags` byte has been present unconditionally since protocol v3. Bit 0
carries `version` + `writer_node_hash` (used by `GET_VERSIONED` responses for
LWW comparison in quorum reads and read repair). Bit 1 carries `expires_at`,
the absolute wall-clock expiry, so TTL semantics survive across nodes. Bit 2
carries `trace_id` + `span_id` for distributed tracing correlation — the
server echoes the client's trace context back and propagates it to replicas.

## Status Codes

| Code | Name | Description |
|------|------|-------------|
| 0 | `OK` | Success |
| 1 | `NotFound` | Key not in cache |
| 2 | `CapacityExceeded` | Value exceeds capacity |
| 3 | `InvalidArgument` | Malformed request — truncated payload, opcode out of range, `expires_at` overflow, or a failed `mustRead` |
| 4 | `TtlExpired` | Key expired |
| 5 | `NotSupported` | Unsupported operation |
| 6 | `InternalError` | Server internal error |
| 7 | `Timeout` | Operation timed out |
| 8 | `NotReady` | Node not ready; body may carry `"moved to <node>"` |

`NotReady` covers two distinct cases: ownership redirects (value field carries
`"moved to <node-id>"`) and failed quorum writes.

## Decode Safety

Decoding goes through `mustRead<T>()`, which returns `Result<T>` instead of
throwing. Truncated payloads, oversized integers, and corrupted headers all
surface as `Result` errors that propagate through the connection handler,
rather than as exceptions (`std::bad_expected_access`) or undefined behavior.

## Worked Example: `SET "foo" "bar"`, 30s TTL

Payload is 35 bytes (without trace) or 51 bytes (with trace). Without trace:

```
flags               0x01                        has_ttl, no expires_at
ttl_ms               00 00 00 1E                30,000
version              00 00 00 00 00 00 00 02     2
writer_node_hash     00 00 00 00 00 00 00 42     66
key_len               00 00 00 03                3
key                    66 6F 6F                  "foo"
val_len                00 00 00 03                3
value                   62 61 72                 "bar"
```

```
Request:
  C1 03 02 00 00 00 23     header: magic=0xC1, v=3, op=SET, len=35
  01                        flags: has_ttl=1
  00 00 00 1E              ttl_ms: 30,000
  00 00 00 00 00 00 00 02  version: 2
  00 00 00 00 00 00 00 42  writer_node_hash: 66
  00 00 00 03              key_len: 3
  66 6F 6F                 key: "foo"
  00 00 00 03              val_len: 3
  62 61 72                 value: "bar"

Response:
  C1 03 00 00 00 00 09     header: magic=0xC1, v=3, op=0, len=9
  00                        status: OK
  00                        flags: no version metadata, no expires_at
  00 00 00 01              has_val: 1
  62 61 72                 value: "bar"
```

With trace (flags = 0x05: has_ttl + has_trace), the request gains 16 bytes
(`trace_id` + `span_id` = 51 bytes total):

```
Request:
  C1 03 02 00 00 00 33     header: magic=0xC1, v=3, op=SET, len=51
  05                        flags: has_ttl=1, has_trace=1
  00 00 00 1E              ttl_ms: 30,000
  00 00 00 00 00 00 00 07  trace_id: 7
  00 00 00 00 00 00 00 0A  span_id: 10
  00 00 00 00 00 00 00 02  version: 2
  00 00 00 00 00 00 00 42  writer_node_hash: 66
  00 00 00 03              key_len: 3
  66 6F 6F                 key: "foo"
  00 00 00 03              val_len: 3
  62 61 72                 value: "bar"

Response:
  C1 03 00 00 00 00 19     header: magic=0xC1, v=3, op=0, len=25
  00                        status: OK
  04                        flags: has_trace=1
  00 00 00 00 00 00 00 07  trace_id: 7 (echoed)
  00 00 00 00 00 00 00 0A  span_id: 10 (echoed)
  00 00 00 01              has_val: 1
  62 61 72                 value: "bar"
```

A `GET_VERSIONED` response would instead set `flags = 0x01` and include the
16 bytes of version metadata between `flags` and `has_val`.

## Trace Context

When `flags & 0x04` is set, 16 bytes of trace context (`trace_id` + `span_id`,
both `uint64`) are appended after the conditional `ttl_ms`/`expires_at_ms`
fields in requests and after `expires_at` in responses. This is
backward-compatible: older nodes that do not recognize bit 2 will read the
extra 16 bytes as part of the next field and reject the frame, while
forward-compatible nodes skip unknown trailing bytes.

Trace context enables end-to-end request correlation across the client, server,
and replication layer. The server generates a `trace_id` when one is not
provided (`trace_id == 0`), echoes it in the response, and propagates it to
replicas on `REPLICATE`/`HINT`.

## Anti-Entropy Payloads

Opcodes 9 and 10 carry custom binary payloads inside the `value` field. All
integers are big-endian.

### Digest (`ANTI_ENTROPY_DIGEST` request, opcode 9)

| Field         | Size | Description |
|---------------|------|--------------|
| `num_buckets` | 4    | `uint32` |
| `bucket[0..N-1]` | 8 each | xxHash3 of one bucket's contents |

Total size: `4 + 8 × num_buckets` bytes. Each bucket's hash is computed by
sorting the keys assigned to it, then hashing `key + version + writer_hash`
per key via xxHash3.

### Digest Response (opcode 9 response)

The partner responds with its own digest followed by its entries for divergent
buckets. The initiator derives the divergent set by comparing both digests:

| Field         | Size | Description |
|---------------|------|--------------|
| `num_buckets` | 4    | `uint32` |
| `bucket[0..N-1]` | 8 each | xxHash3 of one bucket's contents |
| entry count   | 4    | number of entries, `uint32` |
| entries       | var  | variable-length entries (see below) |

### Entry List (`ANTI_ENTROPY_SYNC`, opcode 10)

A 4-byte `count` field, followed by `count` variable-length entries:

| Field              | Size | Description |
|--------------------|------|--------------|
| `key_len`           | 4    | `uint32` |
| `key`               | N    | |
| `version`           | 8    | LWW version, `uint64` |
| `writer_node_hash`  | 8    | `uint64` |
| `has_ttl`           | 1    | `0` or `1` |
| `expires_at_ms`     | 8    | only if `has_ttl == 1` |
| `val_len`           | 4    | `uint32` |
| `value`             | M    | |

## Implementation Reference

Defined in `include/cinder/net/protocol.hpp` and `src/net/protocol.cpp`:

- `net::encode(const Request&) -> Result<vector<byte>>` (`net::encodeInto` for buffer reuse)
- `net::decode(span<const byte>) -> Result<Request>`
- `net::encode(const Response&) -> Result<vector<byte>>` (`net::encodeInto` for buffer reuse)
- `net::decodeResponse(span<const byte>) -> Result<Response>`

Decoded requests are dispatched to `TcpConnection::handleRequest()`, which
routes by opcode to the corresponding store, replication, gossip, or
anti-entropy handler. Each connection runs on its own `asio::strand`, so
concurrent requests on the same connection never interleave.
