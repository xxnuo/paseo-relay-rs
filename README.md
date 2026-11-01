# Paseo Relay

A distributed, protocol-compatible relay for [Paseo](https://github.com/getpaseo/paseo).

Paseo Relay keeps its public WebSocket protocol independent from its deployment platform. Nodes use OTP only for discovery and route ownership. A deployment adapter reroutes WebSocket upgrades to the owning node, so frames stay inside one BEAM node.

This project is under active development and its internal protocols may change without notice.

## Protocol compatibility

`serverId` and v2 `connectionId` are accepted only up to 256 bytes. This is a
deliberate compatibility difference from the Durable Object relay, which does
not impose this relay-side route-key limit. Oversized identifiers receive `400`
before they can create ownership or session state.

## Development

The Elixir and Erlang versions are managed with [asdf](https://asdf-vm.com/):

```sh
asdf install
mix deps.get
mix test
scripts/ci.sh
```

`scripts/ci.sh` runs dependency checks, formatting, tests, the production release
and generic container health, readiness, metrics and restart checks.

See [LICENSE](LICENSE).

## Runtime contract

The release has no deployment-provider dependency. Its settings are intentionally
generic:

| Setting | Default | Meaning |
| --- | --- | --- |
| `PASEO_RELAY_HOST` | `127.0.0.1` | Public listener IP. |
| `PASEO_RELAY_PORT` | `4000` | Public HTTP/WebSocket listener. |
| `PASEO_RELAY_DRAIN` | `false` | Start not-ready while existing sessions drain. |
| `PASEO_RELAY_OWNERSHIP_TARGET` | `local` | Opaque target advertised to other relay nodes. |
| `PASEO_RELAY_REROUTE_HEADER` | `x-reroute-target` | Response header used by the deployment adapter. |
| `PASEO_RELAY_CLUSTER_QUERY` | unset | Optional DNS query used to discover BEAM peers. |
| `PASEO_RELAY_CLUSTER_NODES` | unset | Optional comma-separated `name@host` peers, connected at startup and retried every 5 seconds alongside DNS discovery. |
| `PASEO_RELAY_MIN_CLUSTER_SIZE` | `1` | Minimum nodes required before accepting unowned sessions. |
| `PASEO_RELAY_ACCEPTORS` | `100` | Listener acceptor processes. |
| `PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR` | `200` | Capacity factor multiplied by the acceptor count to set the node-local active-WebSocket ceiling; the default is 20,000. |
| `PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS` | `15000` | Maximum idle time for pre-upgrade HTTP parsing and unread request bodies. Upgraded WebSockets remain exempt. |
| `PASEO_RELAY_CAPACITY_MUTATION_TIMEOUT_MS` | `5000` | Maximum wait for a state-changing Capacity decision before the exact ledger epoch is invalidated. Choose a value appropriate to the runtime. |
| `PASEO_RELAY_INGRESS_BUDGET_BYTES` | `536870912` | Node-wide weighted ceiling for complete WebSocket messages admitted to relay delivery. Must admit one maximum message at the configured weight. |
| `PASEO_RELAY_INGRESS_WEIGHT` | `4` | Conservative memory weight charged per wire payload byte. |
| `PASEO_RELAY_DELIVERY_TIMEOUT_MS` | `30000` | Maximum Writer reservation/write-barrier wait before a slow destination is shed. Also the shared budget for starting delivery of a closing peer's last messages. |
| `PASEO_RELAY_TRANSPORT_SEND_TIMEOUT_MS` | `35000` | TCP send timeout. Must be greater than the Writer deadline so application shedding is recorded before the transport's final fallback. |
| `PASEO_RELAY_CONTROL_QUEUE_BYTES` | `1048576` | Per-destination bound for queued control notifications. |
| `PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS` | `15000` | Maximum time a v2 client frame waits for its daemon-data socket. |
| `PASEO_RELAY_TCP_RECEIVE_BUFFER_BYTES` | `65536` | Per-socket TCP receive buffer. |
| `PASEO_RELAY_WEBSOCKET_MAX_HEAP_WORDS` | `33554432` | Per-WebSocket BEAM heap fuse, including shared binaries. Values below this protocol-safe floor are rejected. |
| `PASEO_RELAY_MEMORY_WATERMARK_BYTES` | `0` | Optional BEAM total-memory watermark that pauses admission and sheds WebSockets with `1013` until measured memory reaches the recovery threshold; disabled generically because the safe threshold depends on the deployment memory limit. |
| `RELEASE_NODE` / `RELEASE_COOKIE` | unset | Standard distributed-release identity. |

`GET /health` is a liveness probe. `GET /ready` returns `200` only while the
node accepts new work, and returns `503 {"status":"unready"}` while draining
or below the configured cluster floor, when the node-local Capacity ledger does
not answer its one-second status observation, during a memory-pressure episode,
or while the application WebSocket ceiling is full. Temporary ingress-byte occupancy does not make the application unready. `GET
/metrics` is Prometheus text and exposes readiness, draining, active WebSockets,
active sessions, reroutes, connection rejections, delivery pressure and latency,
frame sizes, slow-consumer closes, ingress reservations, and BEAM memory for the
local node. If Capacity is unavailable, its four gauge families are omitted
rather than reported as zero.

State-changing Capacity decisions use a separate configured fail-closed
boundary. Each caller captures the current ledger PID; a timeout kills that
exact Capacity epoch, and `:rest_for_one` drains its listener and sockets before
a clean replacement admits traffic. Read-only readiness and metrics status uses
one second and never kills the ledger.

Payloads never pass through a node-wide relay mailbox. Each `serverId` Owner
stores topology metadata only, and every destination has a Writer allowing one
payload write at a time. During delivery Cowboy's native `{active, false}`
flow control suspends source reads while the WebSocket process remains available
for full-duplex writes; the source is rearmed only after the destination's
synchronous HTTP/1 send barrier. One absolute delivery deadline includes Owner
lookup, destination attachment, Writer reservation, and the send barrier;
accepted control notifications are either written within their deadline or
close the destination with retryable `1013`. Kernel TCP pressure therefore
reaches the producer without deadlocking simultaneous opposite-direction
traffic. Cowboy
may finish parsing frames already buffered before suspension, so every completed
message receives an explicit token from one node-local capacity ledger and is
queued in source order. The same ledger owns connection slots, weighted retained
bytes, delivery state, pressure order, and their gauges; a socket monitor, rather
than its termination callback, releases all tokens after abnormal death. Losing
the ledger stops the production listener and its existing connections before a
fresh ledger can reopen admission. Budget exhaustion closes that source with
retryable `1013`.

Client-originated JSON frames with type `hello` or `e2ee_hello` are the narrow
exception to opaque forwarding. After message-capacity admission, a stateless
validation requires the `key` to be canonical padded Base64 for a canonical
32-byte X25519 public key and rejects unsupported values. It checks every
matching text or binary frame on both routing versions; other frames remain
opaque. A rejection is never forwarded and closes the client with `1008 Invalid
handshake key`.

The compatible masked data-frame ceiling remains exactly 32 MiB, which permits
`32 MiB - 14 bytes` of payload. Cowboy applies that payload limit to individual
frames and reassembled fragmented messages and closes an oversized message with
`1009`. The only supported inbound v2 control message is the legacy JSON ping;
control input has a separate 64 KiB Cowboy ceiling and is charged to the same
ledger through parsing. Outbound control notifications use the same Writer
boundary with their own bounded byte queue. Cowboy assembles a complete payload
before the application can request its byte-only Capacity token, and incomplete
fragment assembly remains inside the connection process. The 32 MiB ceiling,
per-WebSocket heap fuse, and configured node memory watermark limit and shed
this staging risk; they do not provide a strict pre-parser memory reservation.
Generic operators should set a nonzero watermark from their runtime limit;
blocked deliveries are shed first.

Build a production release with `MIX_ENV=prod asdf exec mix release`, or build
the generic container with `docker build -t paseo-relay .`.

CI publishes `ghcr.io/getpaseo/paseo-relay` after successful verification on
`main`, tags, and same-repository pull requests. Main also updates
`latest`; pull requests publish only `sha-<commit>` candidate tags. Pin consumers
to the `sha256` digest reported in the publish job summary:

```sh
docker pull ghcr.io/getpaseo/paseo-relay@sha256:<64-hex-digest>
docker run --rm -p 4000:4000 ghcr.io/getpaseo/paseo-relay@sha256:<64-hex-digest>
```

The image and this environment contract are the supported integration boundary.
The container starts the release on port 4000; set `RELEASE_NODE`,
`RELEASE_DISTRIBUTION=name` and `RELEASE_COOKIE` for distributed operation.
Standard OTP settings (`ERL_FLAGS`, `ERL_AFLAGS`, `ERL_EPMD_ADDRESS`) control
VM scheduling and distribution listeners.

Cluster ownership uses [Syn](https://hexdocs.pm/syn/readme.html), an eventually
consistent distributed process registry. A network partition can temporarily
admit one owner for the same `serverId` on each side. When the registry
converges, Syn keeps one owner and the losing owner's WebSockets close with
`1012`, causing clients to reconnect through normal routing. Existing
WebSockets have no transparent migration or cross-partition forwarding. Restore
or fence a prolonged partition before treating reconnects as one session again.

## Rust rewrite (refactor-rust)

The Rust replacement is being developed in `rust/`. The Elixir sources remain
unchanged as the upstream reference and performance baseline; the published image
still runs Elixir. Keep upstream changes as normal Git rebases and compare the
matching modules (`connection.ex` / `connection.rs`, `handshake_validation.ex` /
`protocol.rs`, `ownership.ex` / `session.rs` and `cluster.rs`, `socket.ex` / `relay.rs`). Rust
real-network tests exercise the same public v1/v2 wire contract rather than OTP
implementation details.

Build and start a Linux executable without an Erlang installation:

```sh
cargo build --locked --release --manifest-path rust/Cargo.toml
PASEO_RELAY_HOST=127.0.0.1 PASEO_RELAY_PORT=4000 rust/target/release/paseo-relay
```

For a self-contained Linux amd64 executable, install the Rust
`x86_64-unknown-linux-musl` target and build with
`--target x86_64-unknown-linux-musl`. The resulting
`rust/target/x86_64-unknown-linux-musl/release/paseo-relay` needs no system libc or
Erlang runtime. `deploy/paseo-relay.service` runs `/usr/local/bin/paseo-relay` as
an unprivileged dynamic systemd user and optionally reads
`/etc/paseo-relay/environment`. Its stop timeout includes the relay drain grace.
The generic, nonroot container also contains only the static executable:

```sh
docker build -f rust/Dockerfile -t paseo-relay-rust .
docker run --rm -p 127.0.0.1:4000:4000 paseo-relay-rust
```

Rust implements v1/v2 local routing, control sync/connected/disconnected/ping,
duplicate-role replacement, byte-budget admission, bounded control notifications,
synchronous destination writes and slow-consumer shedding. SIGTERM/SIGINT disables
admission, allows 30 seconds for sessions to drain, then sends retryable `1012`
closes before exit. Health, readiness and core Prometheus counters use the
existing URLs. `PASEO_RELAY_HOST`, `PORT`, `DRAIN`, `ACCEPTORS`,
`CONNECTIONS_PER_ACCEPTOR`, `INGRESS_BUDGET_BYTES`, `INGRESS_WEIGHT`,
`DELIVERY_TIMEOUT_MS`, `TRANSPORT_SEND_TIMEOUT_MS`, `CONTROL_QUEUE_BYTES`,
`DATA_ATTACH_TIMEOUT_MS`, `HTTP_IDLE_TIMEOUT_MS`, `TCP_RECEIVE_BUFFER_BYTES` and
`MEMORY_WATERMARK_BYTES` retain the defaults and bounds above (all names have the
`PASEO_RELAY_` prefix). Acceptor settings determine the connection ceiling; Tokio
schedules I/O. The transport timeout is validated as an upper bound; writes use
the stricter absolute delivery deadline. HTTP idle time includes incomplete
headers and unread bodies; upgraded WebSockets are exempt.

The Rust watermark measures Linux process RSS, not BEAM allocated memory. An
episode pauses admission and readiness, sheds blocked sources first and then
newest remaining sockets, and remeasures RSS every 100 ms between bounded batches.
Admission recovers after RSS falls below the watermark minus one maximum payload.
`paseo_relay_process_resident_memory_bytes` exposes RSS; runtime-specific BEAM
metrics do not apply. A failed enabled RSS observation stops admission, closes
sessions and terminates the service rather than assuming zero memory usage.

Rust supports authenticated static and DNS-discovered clusters. Readiness observes
the configured cluster floor; existing owned sessions continue below that floor,
but new unowned sessions receive `503`. A known remote owner returns the existing
`409` and configured reroute header before local connection or memory admission.
No WebSocket payload crosses the cluster channel. Full metric parity and the
final comparative performance gates remain pending. BEAM heap/distribution
settings do not apply to Rust; the Capacity mutation timeout has no Rust
counterpart because semaphore mutations do not wait on an authority process.
The generic published image still runs the retained Elixir implementation.

### Rust cluster contract

| Setting | Rust behavior |
| --- | --- |
| `PASEO_RELAY_CLUSTER_LISTEN` | Optional dedicated IP:port listener, default `127.0.0.1:4400` when clustering is enabled. Set it to a private reachable interface for multi-host use. |
| `PASEO_RELAY_CLUSTER_NODES` | Comma-separated exact `RELEASE_NODE` identities, `name@host` or `name@host:port`; the default control port is 4400. IPv6 with a port uses brackets. Maximum 1000 configured nodes. |
| `PASEO_RELAY_CLUSTER_QUERY` | Optional DNS name, with optional `:port`, resolved every 5 seconds. Returned endpoints are authenticated before their identity or membership is accepted. |
| `RELEASE_NODE` | Required unique identity when static peers, DNS discovery or the cluster listener is configured. Its advertised host/control port must be reachable by peers. |
| `RELEASE_COOKIE` | Required shared secret of 32–256 bytes for Rust clusters. Debug formatting omits it. |
| `PASEO_RELAY_OWNERSHIP_TARGET` | Required non-default opaque routing target for a cluster node, at most 1024 bytes and valid as an HTTP header value. |
| `PASEO_RELAY_REROUTE_HEADER` | Existing default `x-reroute-target`; must be a valid HTTP header name. |
| `PASEO_RELAY_MIN_CLUSTER_SIZE` | Existing default 1; counts this node and directly authenticated live peers, including transitively discovered peers. |

Peers exchange versioned topology snapshots every 500 ms. Mutual HMAC-SHA256
challenge authentication binds headers and bodies to fresh connection nonces;
replayed packets cannot refresh membership. Each exchange has one two-second
budget. A peer expires after three seconds without fresh authenticated contact;
learned endpoints are retried for 60 seconds, while configured peers and DNS
seeds continue discovery. Snapshots are limited to 16 MiB and reserve a separate
64 MiB staging budget at four bytes per wire byte before allocation/JSON parsing.
At most 128 inbound exchanges run concurrently. Headers are authenticated before
allocating a declared body. This channel authenticates topology metadata; it does
not encrypt it and belongs on the deployment's private network.

Owners retain the original 30-second empty-session grace. Connection leases pin
an owner across upgrades, and idle expiry checks the exact lease generation.
After a partition, the same deterministic claim ordering on every node
(creation timestamp, random epoch, node identity) keeps one owner and fences the losing epoch with explicit `1012` closes. A failed
cluster listener/task or duplicate authenticated `RELEASE_NODE` identity drains
admission and closes existing sockets before service exit. As with Syn, a
partition can temporarily admit duplicate owners and there is no transparent
socket migration.

Rust's cluster protocol is independent of BEAM/Syn. Elixir and Rust nodes cannot
share one cluster: switch the complete relay cluster and let sockets reconnect.
Keep internal-protocol versions compatible across the Rust nodes. The retained
Elixir sources remain the reference for upstream public-protocol changes; Rust's
multi-process tests cover equivalent routing/failure behavior rather than OTP
internals.

### Preliminary local-path performance

The retained Elixir baseline `5f4b763` and Rust `9fef3e8` were measured on the
same Linux host through loopback Docker port mappings. Each fresh container had
a 2-CPU quota, 1 GiB memory limit and two runtime workers; the external client
used four workers. Each pair warmed up for 20 bidirectional exchanges, then ran
for 10 seconds. The table reports medians from three runs per implementation,
with alternating execution order. Latency measures sending a frame until the
other endpoint receives it, not an application round trip.

| v2 workload | Elixir frames/s | Rust frames/s | Rust throughput ratio | p99 ms, Elixir / Rust | Peak container MiB, Elixir / Rust |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 pair, 1 KiB | 24,313 | 46,688 | 1.92 | 0.065 / 0.035 | 114.6 / 5.2 |
| 64 pairs, 1 KiB | 32,615 | 56,382 | 1.73 | 3.685 / 3.699 | 151.5 / 22.9 |
| 64 pairs, 64 KiB | 8,451 | 12,156 | 1.44 | 12.629 / 9.230 | 237.7 / 35.3 |

Every measured frame passed content/order checks; server frame and byte counters
matched the workload including warmup, WebSockets and ingress leases returned to
zero, and no container reported OOM events. `benchmarks/2026-11-01-summary.json`
indexes the medians; the adjacent directories retain load output, before/after
metrics and cgroup observations, image identities and host metadata.
`2026-11-01-provenance.json` records source/binary fingerprints and explains the
pre-commit dirty metadata. Build the common load generator with
`cargo build --locked --release --manifest-path rust/Cargo.toml --example relay_bench`;
`scripts/compare-rust.py --help` describes the repeatable comparison driver
(run it with `uv run python`).

These are closed-loop, single-outstanding-message measurements of the current
local paths. Rust still lacks distributed ownership and complete metric parity;
rerun these gates after those features land. The results establish lower memory
use and higher throughput for these workloads, not a universal latency or
open-loop capacity improvement. In particular, 64-pair 1 KiB p99 did not improve.
