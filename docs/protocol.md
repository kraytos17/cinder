# Cinder Wire Protocol v3

## Overview

Binary, length-prefixed, request-response protocol over TCP.

All multi-byte integers are **big-endian** (network byte order).

## Frame Header (7 bytes)

Every message starts with a fixed 7-byte header:

```
Offset  Size  Field        Description
──────  ────  ───────────  ─────────────────────────────
0       1     magic        0xC1 — identifies a Cinder frame
1       1     version      0x03 — protocol version
2       1     opcode       Request: 1=GET, 2=SET, 3=DEL, 4=PING, 5=GOSSIP,
                           6=REPLICATE, 7=HINT, 8=GET_VERSIONED
                           Response: 0x00
3       4     payload_len  Length of payload in bytes (uint32, big-endian)
```

Maximum total message size: `K_MAX_MESSAGE_SIZE = 67,108,864` (64 MiB). Enforced
on both encode and decode; the decoder rejects any frame whose `payload_len`
exceeds this before reading the body.

The opcode byte is validated on decode: any value outside `GET..GET_VERSIONED`
(1..8) is rejected as an unknown opcode.

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
  storing.
- **`version`** (uint64, big-endian) — logical version for LWW conflict
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

## Response Payload

```
Offset  Size  Field        Description
──────  ────  ───────────  ─────────────────────────────
0       1     status       Errc enum value (see below)
1       4     has_val      Non-zero if a value follows (uint32, big-endian)
5       M     value        Value bytes (only if has_val != 0)
```

For `GET_VERSIONED` responses, the response also carries version metadata after the value:

```
Offset        Size  Field              Description
────────────  ────  ─────────────────  ─────────────────────────────
5+M           8     version            LWW version (uint64, big-endian)
13+M          8     writer_node_hash   Writer node hash (uint64, big-endian)
```

The header opcode byte is always `0x00` on responses.

## Status Codes

| Code | Name | Description |
|------|------|-------------|
| 0 | OK | Success |
| 1 | NotFound | Key not in cache |
| 2 | CapacityExceeded | Value exceeds capacity |
| 3 | InvalidArgument | Malformed request |
| 4 | TtlExpired | Key expired |
| 5 | NotSupported | Unsupported operation |
| 6 | InternalError | Server internal error |
| 7 | Timeout | Operation timed out |
| 8 | NotReady | Node not ready; body may carry `"moved to <node>"` |

`NotReady` is used both for ownership redirects (the server replies
`"moved to <node-id>"` in the value field) and for failed quorum writes.

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
  C1 03 00 00 00 00 05     header: magic=0xC1, v=3, op=0, len=5
  00                        status: OK
  00 00 00 01              has_val: 1
  62 61 72                 value: "bar"
```

## Implementation

- Encoding: `net::encode(const Request&) -> Result<vector<byte>>`
  (and `net::encodeInto` for buffer reuse)
- Decoding: `net::decode(span<const byte>) -> Result<Request>`
- Response encoding: `net::encode(const Response&) -> Result<vector<byte>>`
- Response decoding: `net::decodeResponse(span<const byte>) -> Result<Response>`

All defined in `include/cinder/net/protocol.hpp` and `src/net/protocol.cpp`.
