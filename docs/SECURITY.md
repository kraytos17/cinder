# Security model

Cinder uses TLS 1.2+ for encryption and HMAC bearer tokens for
authentication. There are no per-user identities, ACLs, or revocation;
rotation requires a coordinated restart.

## TLS

- Minimum version is TLS 1.2 by default, TLS 1.3 via `--tls-min-version 1.3`
  (applies to the TCP server, the inter-node pool, and the replication
  pool). Handshakes below the floor fail; rotation of the floor is restart-
  free at startup time only (changing it at runtime redials pooled
  connections, existing connections are unaffected).
- Servers present a chain from `--tls-cert`/`--tls-key`. Client-certificate
  policy is `--tls-client-auth`: `none` (never request), `request` (verify
  when a certificate is presented, allow without), `require` (reject
  handshakes without a valid chained certificate). Default is `require`
  when `--tls-ca` is set, else `none`; `request`/`require` need `--tls-ca`
  (exit 2 otherwise). Rotating the CA still requires a restart; the serving
  chain hot-reloads from disk on every handshake and survives rotation.
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
- Client traffic (`GET`, `SET`, `DEL`, `PING`, `GET_VERSIONED`,
  `COUNTER_*`) and read-only admin (`ADMIN_INFO`, `ADMIN_CLUSTER`,
  `ADMIN_RING`) require the static client token
  `HMAC-SHA256(secret, "cinder-client-v1")` when any client secret is
  configured, accepting the data or the admin bearer. Mutating admin
  (`ADMIN_COMPACT`, `ADMIN_CONFIG_RELOAD`, `ADMIN_SHUTDOWN`,
  `ADMIN_DRAIN`) requires the admin token when `--client-secret-admin`
  is set, else the client token, else the node token. Without an admin
  secret the single client secret keeps full access (backward
  compatible). Set the admin secret with `--client-secret-admin` (or
  `CINDER_CLIENT_SECRET_ADMIN`); like the data bearer it is replayable
  and requires TLS. `cinder-admin` sends the admin bearer when
  configured, falling back to the data bearer.
- Denials return `PermissionDenied` with no value body and bump
  `cinder_auth_failures_total{reason="client"|"node"}` by opcode class.

## Secret rotation

Each secret class takes a previous value accepted alongside the current
one: `--shared-secret-previous`, `--client-secret-previous`,
`--client-secret-admin-previous` (or `CINDER_*_PREVIOUS`, or the matching
`*_previous` JSON config keys, or `*_previous` config-reload keys).
Rotate without a restart:

1. Set the new secret as previous everywhere (config file + `SIGHUP`,
   or `cinder-admin config-reload` with the `*_previous` keys) and wait
   for every node to report it (no errors in the reload path).
2. Promote: make the new value current and keep the old as previous,
   same mechanism. During the window every node accepts both, so mixed
   versions interoperate and in-flight tokens keep verifying.
3. Drop the previous value everywhere once all nodes run the new
   current. Tokens minted under the old secret are denied from then on.

Previous bearers verify exactly like current ones (node windows,
constant-time client compares); rotation never widens the window, it
only temporarily accepts two values. Rotating the CA itself still
requires a restart.

## Abuse resistance

- Frames are header-first: the 7-byte header is read before any payload
  allocation, and `payload_len` is checked against 64 MiB and the 1 MiB
  per-connection staging cap before allocation; oversize frames close the
  connection.
- Decoders validate length-prefixed counts against remaining bytes before
  `make`, so garbage frames cannot amplify allocation.
- Connection writes have a short deadline; slow readers are dropped and
  counted separately from generic write failures.
- Connection budget: `--max-connections` (default 10000, reloadable)
  caps concurrent sockets. Over-limit sockets are closed at accept and
  counted as `cinder_connections_rejected_total`; the accept loop never
  blocks, and lowering the budget only refuses new sockets until the
  count drains.
- Denial logging is sampled: the first and every 64th `PermissionDenied`
  logs at warn with trace IDs and opcode, the rest at debug with
  identical fields. The metric counts every denial regardless of level,
  so a burst cannot spam the log while remaining fully observable.

## Supply chain

- `just vuln` (govulncheck, pinned via `go tool`) runs in CI on every
  push; `go.mod`/`go.sum` exactness is enforced (`go mod tidy` + diff
  gate) and all linters/generators run via pinned `go tool` directives.
- `just build` uses `-trimpath` so local paths never leak into binaries.
- Minimal images, non-root users, and SBOMs belong to the packaging
  step and are explicitly out of scope here.
