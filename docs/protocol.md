# Cinder Wire Protocol v1

## Overview

Binary, length-prefixed, request-response protocol over TCP.

All multi-byte integers are **big-endian** (network byte order).

## Frame Header (7 bytes)

Every message starts with a fixed 7-byte header:

```
Offset  Size  Field        Description
──────  ────  ───────────  ─────────────────────────────
0       1     magic        0xC1 — identifies a Cinder frame
1       1     version      0x01 — protocol version
2       1     opcode       Request: 1=GET, 2=SET, 3=DEL, 4=PING, 5=GOSSIP
                           Response: 0x00
3       4     payload_len  Length of payload in bytes (uint32, big-endian)
```

Maximum total message size: `kMaxMessageSize = 67,108,864` (64 MiB).

## Request Payloads

### SET (opcode=2)

```
Offset  Size  Field        Description
──────  ────  ───────────  ─────────────────────────────
0       1     flags        bit 0: has_ttl
1       4     ttl_ms       TTL in milliseconds (uint32, big-endian, only if flags & 1)
5       4     key_len      Key length in bytes (uint32, big-endian)
9       N     key          Key bytes (UTF-8)
9+N     4     val_len      Value length in bytes (uint32, big-endian)
13+N    M     value        Value bytes
```

### GET (opcode=1)

```
Offset  Size  Field        Description
──────  ────  ───────────  ─────────────────────────────
0       1     flags        0
1       4     key_len      Key length in bytes (uint32, big-endian)
5       N     key          Key bytes (UTF-8)
```

### DEL (opcode=3)

Same layout as GET.

### PING (opcode=4)

```
Offset  Size  Field        Description
──────  ────  ───────────  ─────────────────────────────
0       1     flags        0
1       4     key_len      0
```

### GOSSIP (opcode=5)

Reserved for future use.

## Response Payload

```
Offset  Size  Field        Description
──────  ────  ───────────  ─────────────────────────────
0       1     status       Errc enum value:
                           0=OK, 1=NotFound, 2=CapacityExceeded,
                           3=InvalidArgument, 4=TtlExpired,
                           5=NotSupported, 6=InternalError,
                           7=Timeout, 8=NotReady
1       4     has_val      Non-zero if value follows (uint32, big-endian)
5       M     value        Value bytes (only if has_val != 0)
```

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
| 8 | NotReady | Node not ready |

## Example: SET "foo" "bar" with 30s TTL

```
Hex dump (request):
  C1 01 02 00 00 00 10     header: magic=0xC1, v=1, op=SET, len=16
  01                        flags: has_ttl=1
  00 00 00 1E              ttl_ms: 30,000
  00 00 00 03              key_len: 3
  66 6F 6F                 key: "foo"
  00 00 00 03              val_len: 3
  62 61 72                 value: "bar"

Hex dump (response):
  C1 01 00 00 00 00 05     header: magic=0xC1, v=1, op=0, len=5
  00                        status: OK
  00 00 00 01              has_val: 1
  62 61 72                 value: "bar"
```

## Implementation

- Encoding: `net::encode(const Request&) -> Result<vector<byte>>`
- Decoding: `net::decode(span<const byte>) -> Result<Request>`
- Response encoding: `net::encode(const Response&) -> Result<vector<byte>>`
- Response decoding: `net::decode_response(span<const byte>) -> Result<Response>`

All defined in `include/cinder/net/protocol.hpp` and `src/net/protocol.cpp`.
