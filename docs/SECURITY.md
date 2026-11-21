# Security model

Cinder uses TLS 1.2+ for encryption and HMAC bearer tokens for
authentication. There are no per-user identities, ACLs, or revocation;
rotation requires a coordinated restart.

## TLS

- Minimum version is TLS 1.2 on all listeners (`tlscfg`).
- Servers present a chain from `--tls-cert`/`--tls-key`; `--tls-ca`
  optionally requires verifying client certificates (mTLS).
- External clients (`cinder-cli`, `cinder-admin`) verify both the chain
  and the hostname by default when `--tls-ca` is set: the expected name is
  `--tls-server-name` or falls back to `--host` (DNS SAN or IP SAN via
  `VerifyOptions.DNSName`). Pass `--tls-verify-hostnames=false` only for
  testing; inter-node replication still uses chain-only verification.
- A client secret requires TLS at configure time; node tokens work without
  TLS but are replayable within their window, so run shared-secret
  clusters over TLS.

## Authentication

- Node traffic (`REPLICATE`, `HINT`, `GOSSIP`, `ANTI_ENTROPY_*`,
  `COUNTER_MERGE`, `COUNTER_ANTI_ENTROPY`) requires a time-windowed node
  token: `HMAC-SHA256(shared_secret, node_id + ":" + unix_seconds/10)`,
  accepted for the current bucket plus prior buckets covering 30s and one
  boundary bucket. Receivers accept tokens minted by any alive peer, not
  just themselves.
- Client/admin traffic (`GET`, `SET`, `DEL`, `PING`, `COUNTER_*`,
  `ADMIN_*`) requires the static client token
  `HMAC-SHA256(client_secret, "cinder-client-v1")` when a client secret is
  configured. One secret grants full access, including admin opcodes.
- Denials return `PermissionDenied` with no value body and bump
  `cinder_auth_failures_total{reason="client"|"node"}` by opcode class.

## Abuse resistance

- Frames are header-first: the 7-byte header is read before any payload
  allocation, and `payload_len` is checked against 64 MiB and the 1 MiB
  per-connection staging cap before allocation; oversize frames close the
  connection.
- Decoders validate length-prefixed counts against remaining bytes before
  `make`, so garbage frames cannot amplify allocation.
- Connection writes have a short deadline; slow readers are dropped and
  counted separately from generic write failures.
