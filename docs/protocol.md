# Cinder Wire Protocol v3

## Overview

Binary, length-prefixed, request-response protocol over TCP.

All multi-byte integers are **big-endian** (network byte order).

## Frame Header (7 bytes)

Every message starts with a fixed 7-byte header. The size is derived at
compile time via `consteval` and verified with `static_assert`:

```
Offset  Size  Field        Description
──────  ────  ───────────  ─────────────────────────────
0       1     magic        0xC1 — identifies a Cinder frame
1       1     version      0x03 — protocol version
2       1     opcode       Request: 1=GET, 2=SET, 3=DEL, 4=PING, 5=GOSSIP,
                           6=REPLICATE, 7=HINT, 8=GET_VERSIONED,
                           9=ANTI_ENTROPY_DIGEST, 10=ANTI_ENTROPY_SYNC
                           Response: 0x00
3       4     payload_len  Length of payload in bytes (uint32, big-endian)
```

Maximum total message size: `K_MAX_MESSAGE_SIZE = 67,108,864` (64 MiB). Enforced
on both encode and decode; the decoder rejects any frame whose `payload_len`
exceeds this before reading the body.

The opcode byte is validated on decode: any value outside `Get..AntiEntropySync`
(1..10) is rejected as an unknown opcode. A `consteval` function
`opcodeRangeCoverage()` verifies at compile time that the opcode range is
contiguous with no gaps.

## Request Payload — common format

All requests share the same field sequence. Fields are written in order, with the
two optional fields (`ttl_ms`, `expires_at_ms`) present only when their flag bit
is set — so byte offsets depend on which flags are present:

```
Field            Size  Present when
───────────────  ────  ─────────────────────────────
flags            1     always
ttl_ms           4     flags & 0x01
expires_at_ms    8     flags & 0x02
version          8     always
writer_node_hash 8     always
key_len          4     always
key              N     always (may be empty)
val_len          4     always
value            M     always (may be empty)
```

- **`flags`**: bit 0 = `has_ttl`, bit 1 = `has_expires_at`.
- **`ttl_ms`** (uint32, big-endian) — relative TTL in milliseconds, sent by
  **clients** on `SET`. The server converts it to an absolute expiry using its own
  clock.
- **`expires_at_ms`** (uint64, big-endian) — absolute wall-clock expiry, unix epoch
  milliseconds, sent by the **primary** on `REPLICATE`/`HINT`. Because it is
  absolute, every replica expires the key at the same instant regardless of
  delivery delay. Replicas convert it to their local steady-clock basis before
  storing. **Overflow guard**: values above `int64_t::max() / 1,000,000`
  (~9.2 × 10¹² ms ≈ year 2262) are rejected to prevent undefined behavior when
  the `milliseconds` → `time_point` conversion multiplies by 1,000,000 for
  nanosecond resolution.
- **`version`** (uint64, big-endian) — monotonic version for LWW conflict
  resolution. Meaningful on `SET`/`REPLICATE`/`HINT`; 0 elsewhere.
- **`writer_node_hash`** (uint64, big-endian) — stable per-node writer hash used
  to break version ties. Meaningful on writes; 0 elsewhere.

`ttl_ms` and `expires_at_ms` are semantically exclusive: clients send the former,
replication uses the latter.

## Opcode-Specific Notes

| Opcode | Field usage |
|--------|-------------|
| GET (1) | `key` set; no ttl/expires_at; value empty |
| SET (2) | `key` + `value` set; `ttl_ms` when a relative TTL is given; `version`/`writer_node_hash` may carry LWW metadata |
| DEL (3) | `key` set |
| PING (4) | all fields empty/zero |
| GOSSIP (5) | membership view in `value`: `;`-delimited `id@host:port:state:incarnation` entries, e.g. `node1@127.0.0.1:7000:alive:3;node2@127.0.0.1:7001:dead:7`; `key` empty |
| REPLICATE (6), HINT (7) | `key` + `value` set; `expires_at_ms` when the primary computed an absolute expiry; `version` + `writer_node_hash` carry LWW metadata |
| GET_VERSIONED (8) | `key` set; response carries `version` + `writer_node_hash` for LWW comparison (used by quorum reads and read repair) |
| ANTI_ENTROPY_DIGEST (9) | Initiator sends its bucket digest (see below); `key` empty |
| ANTI_ENTROPY_SYNC (10) | Initiator sends entries for divergent buckets (see below); `key` empty |

## Response Payload

The response payload is variable-length. After the 7-byte frame header:

```
Field              Size  Present when
────────────────   ────  ─────────────────────────────
status             1     always
flags              1     always (bit 0 = version metadata; bit 1 = expires_at)
version            8     flags & 0x01
writer_node_hash   8     flags & 0x01
expires_at         8     flags & 0x02
has_val            4     always
value              M     has_val != 0
```

The flags byte is always present (from protocol v3 onwards). When bit 0 is set,
the response carries `version` + `writer_node_hash` (used by `GET_VERSIONED`
responses for LWW comparison in quorum reads and read repair). When bit 1 is
set, `expires_at` carries the absolute wall-clock expiry so TTL semantics are
preserved across nodes.

The header opcode byte is always `0x00` on responses.

## Status Codes

| Code | Name | Description |
|------|------|-------------|
| 0 | OK | Success |
| 1 | NotFound | Key not in cache |
| 2 | CapacityExceeded | Value exceeds capacity |
| 3 | InvalidArgument | Malformed request (truncated payload, opcode out of range, `expires_at` overflow, `mustRead` failure) |
| 4 | TtlExpired | Key expired |
| 5 | NotSupported | Unsupported operation |
| 6 | InternalError | Server internal error |
| 7 | Timeout | Operation timed out |
| 8 | NotReady | Node not ready; body may carry `"moved to <node>"` |

`NotReady` is used both for ownership redirects (the server replies
`"moved to <node-id>"` in the value field) and for failed quorum writes.

## Decode Safety

The decode path uses `mustRead<T>()` which returns `Result<T>` instead of
throwing. This prevents `std::bad_expected_access` on adversarial or malformed
frames — truncated payloads, oversized integers, and corrupted headers are all
handled as `Result` errors that propagate up through the connection handler.

## Example: SET "foo" "bar" with 30s TTL

Wire fields (payload = 35 bytes):

```
flags               0x01                        has_ttl, no expires_at
ttl_ms              00 00 00 1E                 30,000
version             00 00 00 00 00 00 00 02     2
writer_node_hash    00 00 00 00 00 00 00 42     66
key_len             00 00 00 03                 3
key                 66 6F 6F                    "foo"
val_len             00 00 00 03                 3
value               62 61 72                    "bar"
```

```
Hex dump (request):
  C1 03 02 00 00 00 23     header: magic=0xC1, v=3, op=SET, len=35
  01                        flags: has_ttl=1
  00 00 00 1E              ttl_ms: 30,000
  00 00 00 00 00 00 00 02  version: 2
  00 00 00 00 00 00 00 42  writer_node_hash: 66
  00 00 00 03              key_len: 3
  66 6F 6F                 key: "foo"
  00 00 00 03              val_len: 3
  62 61 72                 value: "bar"

Hex dump (response):
  C1 03 00 00 00 00 06     header: magic=0xC1, v=3, op=0, len=6
  00                        status: OK
  00                        flags: no version metadata, no expires_at
  00 00 00 01              has_val: 1
  62 61 72                 value: "bar"
```

A `GET_VERSIONED` response would set flags = `0x01` and include the 16 bytes of
version metadata after the flags byte (before `has_val`).

## Anti-Entropy Binary Formats

The anti-entropy protocol (opcodes 9 and 10) uses custom binary payloads
carried in the `value` field. All integers are big-endian.

### Digest (opcode 9 — `ANTI_ENTROPY_DIGEST`)

```
Field         Size     Description
───────────   ──────   ─────────────────────────────────
num_buckets   4        Number of hash buckets (uint32)
bucket[0]     8        xxHash3 of keys in bucket 0
bucket[1]     8        xxHash3 of keys in bucket 1
...          ...       ...
bucket[N-1]   8        xxHash3 of keys in bucket N-1
```

Total size: `4 + 8 × num_buckets` bytes. The digest is computed by sorting
keys within each bucket, then hashing key+version+writer_hash via xxHash3.

### Divergent Bucket IDs (opcode 9 response)

```
Field         Size     Description
───────────   ──────   ─────────────────────────────────
count         4        Number of divergent buckets (uint32)
id[0]         4        Bucket ID (uint32)
id[1]         4        Bucket ID (uint32)
...          ...       ...
```

### Entry List (opcode 10 — `ANTI_ENTROPY_SYNC`)

Each entry in the `value` payload is serialized as:

```
Field              Size  Description
────────────────   ────  ─────────────────────────────────
key_len            4     Key length (uint32)
key                N     Key bytes
version            8     LWW version (uint64)
writer_node_hash   8     Writer node hash (uint64)
has_ttl            1     0 or 1 (uint8)
expires_at_ms      8     Absolute expiry ms (only if has_ttl == 1)
val_len            4     Value length (uint32)
value              M     Value bytes
```

The entry list is preceded by a 4-byte `count` field, then `count` entries
are serialized contiguously. Each entry is variable-length due to key/value
sizes and the optional `expires_at_ms`.

## Implementation

- Encoding: `net::encode(const Request&) -> Result<vector<byte>>`
  (and `net::encodeInto` for buffer reuse)
- Decoding: `net::decode(span<const byte>) -> Result<Request>`
- Response encoding: `net::encode(const Response&) -> Result<vector<byte>>`
  (and `net::encodeInto` for buffer reuse)
- Response decoding: `net::decodeResponse(span<const byte>) -> Result<Response>`

All defined in `include/cinder/net/protocol.hpp` and `src/net/protocol.cpp`.

Decoded requests are delivered to the `TcpConnection::handleRequest()` handler,
which dispatches by opcode to the appropriate store/replication/gossip/anti-entropy
handler. Each connection is serialized on its own `asio::strand`, so concurrent
requests on the same connection do not interleave.
