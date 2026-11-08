# Cinder Wire Protocol v6

Binary, length-prefixed, request-response protocol over TCP. All multi-byte
integers on the wire are **big-endian** (network byte order). On-disk formats
(snapshot, WAL) use native byte order.

## Frame Header

Every message begins with a fixed 7-byte header. Its size is derived at
compile time via `consteval` and verified with `static_assert`.

| Offset | Size | Field         | Description |
|--------|------|---------------|--------------|
| 0      | 1    | `magic`       | `0xC1` — identifies a Cinder frame |
| 1      | 1    | `version`     | `0x06` |
| 2      | 1    | `opcode`      | Request: 1–19 (see [Opcodes](#opcodes)). Response: `0x00` |
| 3      | 4    | `payload_len` | Payload length in bytes, `uint32` |

**Limits and validation:**
- Max total message size: `K_MAX_MESSAGE_SIZE = 67,108,864` (64 MiB), enforced on
  both encode and decode. On decode, `payload_len` is checked against this
  bound before the body is read.
- `opcode` must fall in `1..19`; any other value is rejected as unknown. A
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
| `version`           | 8    | always |
| `writer_node_hash`  | 8    | always |
| `key_len`           | 4    | always |
| `key`               | N    | always (may be empty) |
| `val_len`           | 4    | always |
| `value`             | M    | always (may be empty) |
| `counter_delta`      | 8    | `COUNTER_ADJUST` only |
| `counter_decrement`  | 1    | `COUNTER_ADJUST` only (0 = increment, 1 = decrement) |
| `trace_id`          | 8    | `flags & 0x04` |
| `span_id`           | 8    | `flags & 0x04` |
| `auth_token`        | 32   | `flags & 0x08` |

- **`flags`** — bit 0: `has_ttl`; bit 1: `has_expires_at`; bit 2: `has_trace`; bit 3: `has_auth`; bit 4: `tombstone` (delete marker on `REPLICATE`/`HINT` — the value is empty and receivers apply a versioned delete).
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
- **`auth_token`** (32 bytes) — HMAC-SHA256 authentication token. When
  `shared_secret` is configured on the server, internal replication opcodes
  (REPLICATE, HINT, GOSSIP, ANTI_ENTROPY_*) must include a node token:
  `HMAC-SHA256(shared_secret, node_id + ":" + unix_seconds/10)`, truncated
  to 10-second windows for clock-skew tolerance. When `client_shared_secret`
  is configured, client opcodes (GET, GET_VERSIONED, SET, DEL, PING) and
  ADMIN_* must instead include a client token:
  `HMAC-SHA256(client_shared_secret, "cinder-client-v1")`. The wire field
  carries exactly one token; the opcode class selects which secret is
  verified. When both secrets are empty, the field is omitted and no
  authentication is performed.
- **`version`** (`uint64`) — monotonic LWW version. Meaningful on
  `SET`/`REPLICATE`/`HINT`; `0` elsewhere.
- **`writer_node_hash`** (`uint64`) — stable per-node hash used to break LWW
  version ties. Meaningful on writes; `0` elsewhere.
- **`counter_delta`** (`uint64`) and **`counter_decrement`** (`uint8`) are
  appended after `value` on `COUNTER_ADJUST`; decrement must be 0 or 1.
  Counter state itself is encoded in `value` using the `CRDC` G/PN-Counter
  encoding. Counter adjustments are not safe to retry after a transport
  error: the owner may have applied the operation even if the response was
  lost. Replica state merge is safe to retry because it is idempotent.

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
| 6 | `REPLICATE` | `key` + `value` set; `expires_at_ms` set when the primary computed an absolute expiry; `version` + `writer_node_hash` carry LWW metadata; `tombstone` flag set for versioned deletes (value empty) |
| 7 | `HINT` | same layout as `REPLICATE` (tombstone flag included) |
| 8 | `GET_VERSIONED` | `key` set; response carries `version` + `writer_node_hash` for LWW comparison (quorum reads, read repair) plus a `tombstone` flag when the key is deleted (no value) |
| 9 | `ANTI_ENTROPY_DIGEST` | initiator sends its group digest, later a bucket refinement for divergent groups (`key` = `bucket-refinement-v3`); see [Anti-Entropy Payloads](#anti-entropy-payloads) |
| 10 | `ANTI_ENTROPY_SYNC` | entries where the sender wins plus a needlist of key hashes where it loses (partner answers with entries); `key` empty |
| 11 | `ADMIN_INFO` | request: empty; response: `value` = JSON node info (node_id, port, capacity, store stats, config) |
| 12 | `ADMIN_CLUSTER` | request: empty; response: `value` = JSON membership array (id, host, port, state, incarnation) |
| 13 | `ADMIN_RING` | request: empty; response: `value` = JSON ring snapshot (self, vnodes_per_node) |
| 14 | `ADMIN_COMPACT` | request: empty; response: `status` = OK on success |
| 15 | `ADMIN_CONFIG_RELOAD` | request: empty; response: `status` = OK on success |
| 16 | `ADMIN_SHUTDOWN` | request: empty; response: `status` = OK, then server shuts down gracefully |
| 17 | `COUNTER_ADJUST` | client-only, owner-routed; `key`, `counter_delta`, `counter_decrement`; response carries the updated serialized PN-Counter state |
| 18 | `COUNTER_MERGE` | internal-only; `key` + serialized PN-Counter state in `value`; receiver merges, never LWW-replaces |
| 19 | `COUNTER_GET` | client read; response `value` is serialized PN-Counter state, `NotFound` if absent |

Admin opcodes (11-16) bypass ring ownership checks and are client-initiated only.
`COUNTER_MERGE` is internal/authenticated like replication; counter adjust/get
are client-authenticated and owner-routed. Counter state is asynchronously
replicated as full PN-Counter snapshots. Counter-aware anti-entropy and WAL/
snapshot durability are not yet implemented.

## Response Payload

The opcode byte on a response frame is always `0x00`.

| Field              | Size | Present when |
|--------------------|------|---------------|
| `status`            | 1    | always |
| `flags`             | 1    | always (bit 0: version metadata; bit 1: `expires_at`; bit 2: trace; bit 4: `tombstone`) |
| `version`           | 8    | `flags & 0x01` |
| `writer_node_hash`  | 8    | `flags & 0x01` |
| `expires_at`        | 8    | `flags & 0x02` |
| `has_val`           | 4    | always |
| `value`             | M    | `has_val != 0` |
| `trace_id`          | 8    | `flags & 0x04` |
| `span_id`           | 8    | `flags & 0x04` |

The `flags` byte has been present unconditionally since protocol v3. Bit 0
carries `version` + `writer_node_hash` (used by `GET_VERSIONED` responses for
LWW comparison in quorum reads and read repair). Bit 1 carries `expires_at`,
the absolute wall-clock expiry, so TTL semantics survive across nodes. Bit 2
carries `trace_id` + `span_id` for distributed tracing correlation — the
server echoes the client's trace context back and propagates it to replicas.
Bit 4 carries the `tombstone` marker: a `GET_VERSIONED` response for a deleted
key reports `OK` with no value, the delete's `(version, writer)`, and the
flag set, so quorum voters and read repair distinguish "deleted at version V"
from "absent". Plain `GET` on a tombstone returns `NotFound`.

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
| 8 | `NotReady` | Node not ready; body may carry `"moved to <node-id>[@<host>:<port>]"` |
| 9 | `PermissionDenied` | Auth failed — missing or invalid `auth_token` on an internal opcode |
| 10 | `CorruptData` | Persisted data failed validation — bad snapshot magic/version or truncated snapshot entry |

`NotReady` covers two distinct cases: ownership redirects (value field carries
`"moved to <node-id>"`, with `@<host>:<port>` appended when the server knows a
reachable address for the owner so unconfigured clients can follow it) and
failed quorum writes.

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
  C1 05 02 00 00 00 23     header: magic=0xC1, v=5, op=SET, len=35
  01                        flags: has_ttl=1
  00 00 00 1E              ttl_ms: 30,000
  00 00 00 00 00 00 00 02  version: 2
  00 00 00 00 00 00 00 42  writer_node_hash: 66
  00 00 00 03              key_len: 3
  66 6F 6F                 key: "foo"
  00 00 00 03              val_len: 3
  62 61 72                 value: "bar"

Response:
  C1 05 00 00 00 00 09     header: magic=0xC1, v=5, op=0, len=9
  00                        status: OK
  00                        flags: no version metadata, no expires_at
  00 00 00 01              has_val: 1
  62 61 72                 value: "bar"
```

With trace (flags = 0x05: has_ttl + has_trace), the request gains 16 bytes
(`trace_id` + `span_id` = 51 bytes total):

```
Request:
  C1 05 02 00 00 00 33     header: magic=0xC1, v=5, op=SET, len=51
  05                        flags: has_ttl=1, has_trace=1
  00 00 00 1E              ttl_ms: 30,000
  00 00 00 00 00 00 00 02  version: 2
  00 00 00 00 00 00 00 42  writer_node_hash: 66
  00 00 00 03              key_len: 3
  66 6F 6F                 key: "foo"
  00 00 00 03              val_len: 3
  62 61 72                 value: "bar"
  00 00 00 00 00 00 00 07  trace_id: 7
  00 00 00 00 00 00 00 0A  span_id: 10

Response:
  C1 05 00 00 00 00 19     header: magic=0xC1, v=5, op=0, len=25
  00                        status: OK
  04                        flags: has_trace=1
  00 00 00 01              has_val: 1
  62 61 72                 value: "bar"
  00 00 00 00 00 00 00 07  trace_id: 7 (echoed)
  00 00 00 00 00 00 00 0A  span_id: 10 (echoed)
```

A `GET_VERSIONED` response would instead set `flags = 0x01` and include the
16 bytes of version metadata between `flags` and `has_val`.

## Worked Example: Auth Token on `REPLICATE`

With trace + auth (flags = 0x0C: has_trace + has_auth), the request gains
16 bytes (trace) + 32 bytes (auth) = 48 extra bytes:

```
Request:
  C1 05 06 00 00 00 57     header: magic=0xC1, v=5, op=REPLICATE, len=87
  0C                        flags: has_trace=1, has_auth=1
  00 00 00 00 00 00 00 02  version: 2
  00 00 00 00 00 00 00 42  writer_node_hash: 66
  00 00 00 03              key_len: 3
  66 6F 6F                 key: "foo"
  00 00 00 03              val_len: 3
  62 61 72                 value: "bar"
  00 00 00 00 00 00 00 07  trace_id: 7
  00 00 00 00 00 00 00 0A  span_id: 10
  AB AB AB AB AB AB AB AB  auth_token: 32-byte HMAC-SHA256
  AB AB AB AB AB AB AB AB
  AB AB AB AB AB AB AB AB
  AB AB AB AB AB AB AB AB
```

The auth token is appended after trace context at the end of the payload. The
server validates it against the configured `shared_secret` before processing
the request.

## Trace Context

When `flags & 0x04` is set, 16 bytes of trace context (`trace_id` + `span_id`,
both `uint64`) are appended at the end of the payload, after the `value`
field in requests and after `value` in responses. This placement ensures
backward compatibility: old nodes validate the fixed-layout prefix and reject
frames with unknown trailing data, while new nodes extract the optional
trailing context.

Trace context enables end-to-end request correlation across the client, server,
and replication layer. The server generates a `trace_id` when one is not
provided (`trace_id == 0`), echoes it in the response, and propagates it to
replicas on `REPLICATE`/`HINT`.

## Node Authentication

When `flags & 0x08` is set, 32 bytes of authentication data (`auth_token`,
an HMAC-SHA256 digest) are appended at the end of the payload, after trace
context when present. Old nodes validate the fixed-layout prefix and reject
frames with unknown trailing data.

Authentication is enforced per opcode class (see the `auth_token` field
description). Internal replication opcodes (REPLICATE, HINT, GOSSIP,
ANTI_ENTROPY_DIGEST, ANTI_ENTROPY_SYNC) require the node token when
`shared_secret` is configured. Client opcodes (GET, GET_VERSIONED, SET, DEL,
PING) require the client token when `client_shared_secret` is configured.
ADMIN_* requires the client token when client auth is enabled, otherwise the
node token when node auth is configured. Auth is checked before ring-ownership
redirects or any side effects; failures return `PermissionDenied` (9)
without revealing which check failed.

The node token is computed as:
```
node_token = HMAC-SHA256(shared_secret, node_id + ":" + (unix_seconds / 10))
```

The timestamp is truncated to 10-second windows so nodes with slight clock
skew (up to ~30 seconds) can still authenticate. The server checks the
current window and the two preceding windows.

The client token is static and domain-separated:
```
client_token = HMAC-SHA256(client_shared_secret, "cinder-client-v1")
```

The raw secret never goes on the wire. The token is replayable by design,
so the server refuses to enable client auth without a crypto build and TLS
(`cinderd` startup fails otherwise), and the gRPC gateway serves it only
over TLS with the same bearer presented as hex in the `authorization`
metadata (`Bearer <hex>`), verified at every RPC entry point.

When a secret is empty or not configured, no authentication is performed
for its opcode class and the `auth_token` field is omitted from those
requests. Rotation in v1 requires a coordinated restart; there are no
per-user identities, ACLs, or revocation.

Configuration:
```yaml
auth:
  shared_secret: "my-cluster-secret"          # node auth (empty = disabled)
  client_shared_secret: "my-client-secret"    # client auth (empty = disabled, requires TLS)
```

Or via CLI: `cinderd --shared-secret "my-cluster-secret" --client-shared-secret "my-client-secret"`
(`cinder-cli`/`cinder-admin` take `--client-secret`, or `CINDER_CLIENT_SECRET`).

A node with `shared_secret` configured will reject any internal request that
is missing a valid `auth_token` with status code `PermissionDenied` (9).

### Version Bump

Protocol v6 (introduced in this version) is **not backward-compatible** with
v5. The version check uses strict equality — a v6 node will reject v5 frames
and vice versa. All nodes in a cluster must be upgraded simultaneously.

Protocol v6 adds counter opcodes 17–19 and the `COUNTER_ADJUST` suffix fields.
Nodes running v5 reject v6 frames at the version check. All nodes in a cluster
must be upgraded simultaneously.

## Anti-Entropy Payloads

Opcodes 9 and 10 carry custom binary payloads inside the `value` field. All
integers are little-endian (native byte order; the cluster is homogeneous).
Every AE payload starts with a schema magic (`uint32`): `0x41454D33`
("AEM3", group digest request), `0x41454D32` ("AEM2", legacy full digest —
accepted on decode for compatibility but no longer sent), `0x41454D53`
("AEMS", sync), `0x41454D4E` ("AEMN" — reserved), `0x41455236` ("AER6",
digest response), `0x41454235` ("AEB5", bucket refinement). Payloads
without a recognized magic are rejected, so old peers fail a round cleanly
and converge once the cluster is homogeneous. A new layout gets a new magic
and keeps its old decoder while any supported peer may send it; retirement
means deleting the old branch.

### Group Digest (`ANTI_ENTROPY_DIGEST` request, opcode 9)

The initiator sends one xxHash per *group* of 16 consecutive buckets
(`K_BUCKETS_PER_GROUP`), so a converged round costs 16 hashes (136 bytes)
instead of 256 bucket hashes:

| Field         | Size | Description |
|---------------|------|--------------|
| `magic`       | 4    | `0x41454D33` (`uint32`) |
| `group count` | 4    | `uint32` |
| `group[0..G-1]` | 8 each | XOR-fold of the group's bucket hashes |

### Digest Response (opcode 9 response)

The partner answers with its own group hashes only (no bucket data yet):

| Field         | Size | Description |
|---------------|------|--------------|
| `magic`       | 4    | `0x41455236` (`uint32`) |
| `group count` | 4    | `uint32` |
| `group[0..G-1]` | 8 each | XOR-fold of the group's bucket hashes |
| `bucket hash count` | 4 | always `0` in a phase-1 response |
| `covered count` | 4 | always `0` in a phase-1 response |
| `rows size`   | 4    | always `4` (empty metadata row list) |
| rows          | 0    | empty |
| `IBLT count`  | 4    | count of optional bucket sketches (`uint32`) |
| sketches      | var  | repeated `[bucket u32][size u32][IBLT blob]` |

### Bucket Refinement (opcode 9 with key `"bucket-refinement-v3"`)

For each divergent group, the initiator requests that group's bucket
hashes. The request lists the initiator's own hashes so the responder
returns metadata only for buckets that actually differ:

| Field           | Size | Description |
|-----------------|------|--------------|
| `magic`         | 4    | `0x41454235` (`uint32`) |
| `flags`         | 4    | bit 0 forces P2 metadata fallback; other bits invalid |
| `bucket count`  | 4    | `uint32` |
| entries         | 12 each | `[bucket u32][hash u64]` (initiator's view) |

The refinement response reuses the digest-response envelope, this time
populated: the responder's bucket hashes for the requested buckets,
the covered-bucket set, and the metadata rows:

| Field         | Size | Description |
|---------------|------|--------------|
| `magic`       | 4    | `0x41455236` (`uint32`) |
| `group count` | 4    | `uint32` (full group vector, for validation) |
| `group[0..G-1]` | 8 each | responder's group hashes |
| `bucket hash count` | 4 | number of bucket hashes, `uint32` |
| `bucket hashes` | 12 each | `[bucket u32][hash u64]` |
| `covered count` | 4  | number of covered buckets, `uint32` |
| `covered`     | 4 each | `uint32` bucket ids with complete metadata |
| `rows size`   | 4    | metadata bytes that follow, `uint32` |
| rows          | var  | metadata rows (see below) |
| `IBLT count`  | 4    | count of optional bucket sketches (`uint32`) |
| sketches      | var  | repeated `[bucket u32][size u32][IBLT blob]` |

An IBLT blob starts with `0x41454934` (`AEI4`), a cell count, then fixed
48-byte cells: signed count (`i64`, two's-complement), key-hash XOR (`u64`),
version XOR (`u64`), writer XOR (`u64`), tombstone XOR (`u64`), and
item-checksum XOR (`u64`). Three distinct cells are selected per
`(key_hash, version, writer, tombstone)` tuple — the tombstone bit is part of
the item identity, so a live write and its delete never cancel each other
during peeling; the enclosing refinement identifies the bucket. The initiator
subtracts a local sketch and peels pure cells.
Successful peeling yields the exact symmetric difference of LWW identities;
failure triggers an explicit P2 metadata retry for that bucket. The current
threshold is 32 rows per bucket with 64 cells. Smaller buckets carry metadata
rows directly. Sketches are an optimization only; P2 remains the fallback.

A metadata row is `[bucket u32][key_hash u64][version u64][writer u64][tombstone u8]`:
identity plus recency plus the delete marker, no value. Tombstoned rows
participate in LWW with the same `(version, writer)` order as live rows, so
deletes heal through the same diff path as writes. Rows are bucket-atomic (a
bucket's rows are all included or the bucket is deferred to a later round),
so truncation delays but never stalls convergence. Covered buckets with no
rows are explicitly empty — the initiator must not treat omission as absence.

### Sync (`ANTI_ENTROPY_SYNC`, opcode 10)

One format for requests and responses: entries to apply, then a needlist of
key hashes the sender is missing. Responses always carry an empty needlist.

| Field         | Size | Description |
|---------------|------|--------------|
| `magic`       | 4    | `0x41454D53` (`uint32`) |
| `entry count` | 4    | number of entries, `uint32` |
| entries       | var  | variable-length entries (see below) |
| `need count`  | 4    | number of key hashes, `uint32` |
| `need hashes` | 8 each | `uint64` key hashes to fetch |

The initiator sends entries where it wins plus a needlist where it loses
(both byte-budgeted); the partner applies the entries and answers with the
requested entries. Full entries cross only for true deltas.

### Entry (variable-length, inside sync payloads)

| Field              | Size | Description |
|--------------------|------|--------------|
| `key_len`           | 4    | `uint32` |
| `key`               | N    | |
| `version`           | 8    | LWW version, `uint64` |
| `writer_node_hash`  | 8    | `uint64` |
| `entry_flags`       | 1    | bitfield: bit 0 = `has_ttl`, bit 1 = `tombstone`; other bits invalid |
| `expires_at_ms`     | 8    | only if `has_ttl` is set |
| `val_len`           | 4    | `uint32` |
| `value`             | M    | empty for tombstones |

Tombstone entries carry no value and no TTL of their own; instead
`expires_at_ms` holds the GC deadline (reused TTL wheel machinery — see
[Deletes](#deletes) in `rebalance.md`), so replicas reap the marker at the
same instant.

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
