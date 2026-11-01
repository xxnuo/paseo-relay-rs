# Paseo Relay

A distributed, protocol-compatible relay for [Paseo](https://github.com/getpaseo/paseo),
implemented in Rust as one Linux executable. The default Dockerfile and image
publishing pipeline build Rust. The Elixir implementation remains at its upstream
source paths as a tested compatibility and performance reference, not a runtime
dependency. Frames remain end-to-end encrypted; the relay routes opaque payloads.

## Build and Linux deployment

```sh
cargo build --locked --release --manifest-path rust/Cargo.toml
PASEO_RELAY_HOST=127.0.0.1 PASEO_RELAY_PORT=4000 rust/target/release/paseo-relay
```

The executable accepts environment configuration and needs no Erlang installation.
The normal host build uses the host libc. A self-contained Linux amd64 build is:

```sh
rustup target add x86_64-unknown-linux-musl
cargo build --locked --release --manifest-path rust/Cargo.toml --target x86_64-unknown-linux-musl
```

Use `rust/target/x86_64-unknown-linux-musl/release/paseo-relay` on the server.
`deploy/paseo-relay.service` runs `/usr/local/bin/paseo-relay` as an unprivileged
systemd dynamic user, optionally reads `/etc/paseo-relay/environment`, and allows
40 seconds for shutdown. Set the host and memory watermark for the deployment.
The repository contains no provider inventories or proxy/operator configuration.

```sh
docker build -t paseo-relay-rust .
docker run --rm -p 127.0.0.1:4000:4000 paseo-relay-rust
```

The scratch image contains only the static executable and runs as UID/GID 65532.
It listens on 0.0.0.0:4000; native defaults are loopback. CI verifies tests, image
restart and a private two-container cluster, then retains the standalone amd64
binary with SHA256SUMS as a workflow artifact. Successful main/tag/same-repository
PR workflows are configured to publish `ghcr.io/getpaseo/paseo-relay`; pin a
consumer to the digest reported by that workflow. Existing registry images are
not changed just by checking out this branch. See [LICENSE](LICENSE).

## Runtime contract

All rows below have the `PASEO_RELAY_` prefix unless named otherwise.

| Setting | Default | Meaning / accepted bounds |
| --- | --- | --- |
| `HOST` | `127.0.0.1` | Public listener IP address. |
| `PORT` | `4000` | HTTP/WebSocket port, 1–65535. |
| `DRAIN` | `false` | Start unready and reject new upgrades; true/false. |
| `ACCEPTORS` | `100` | Maximum pre-upgrade HTTP connections, 1–1000; also a factor in the WebSocket ceiling. Tokio schedules I/O. |
| `CONNECTIONS_PER_ACCEPTOR` | `200` | Second ceiling factor, 1–1000000. The default active WebSocket ceiling is 20000. |
| `HTTP_IDLE_TIMEOUT_MS` | `15000` | Incomplete headers/unread bodies/HTTP keepalive idle limit, 100–120000 ms. Upgraded sockets are exempt and release their HTTP slot immediately. |
| `INGRESS_BUDGET_BYTES` | `536870912` | Weighted complete-message ceiling, 128 MiB–8 GiB; must admit one maximum message at its configured weight. |
| `INGRESS_WEIGHT` | `4` | Charge per payload byte, 1–16. |
| `DELIVERY_TIMEOUT_MS` | `30000` | Absolute attachment/queue/write budget, 100–120000 ms. |
| `TRANSPORT_SEND_TIMEOUT_MS` | `35000` | Compatibility upper bound, 100–300000 ms; must exceed delivery timeout. Rust writes use the stricter delivery deadline. |
| `CONTROL_QUEUE_BYTES` | `1048576` | Per-control-destination byte queue, 64 bytes–64 MiB. |
| `DATA_ATTACH_TIMEOUT_MS` | `15000` | v2 client wait for daemon data, 1000–120000 ms, capped by the delivery deadline. |
| `TCP_RECEIVE_BUFFER_BYTES` | `65536` | Per-socket receive buffer, 4 KiB–1 MiB. |
| `MEMORY_WATERMARK_BYTES` | `0` | Disabled, or 256 MiB–64 GiB Linux process RSS watermark. Choose it from the real memory limit. |
| `OWNERSHIP_TARGET` | `local` | Opaque reroute target, valid HTTP header value up to 1024 bytes; required non-default in a cluster. |
| `REROUTE_HEADER` | `x-reroute-target` | Valid HTTP response header name. |
| `MIN_CLUSTER_SIZE` | `1` | Required live membership for unowned sessions, 1–1000. |
| `CLUSTER_NODES` | unset | Static peer identities; see below. |
| `CLUSTER_QUERY` | unset | DNS discovery seed; see below. |
| `CLUSTER_LISTEN` | `127.0.0.1:4400` when enabled | Separate private control listener; configuring it enables clustering. |
| `RELEASE_NODE` / `RELEASE_COOKIE` | unset | Rust cluster identity/shared secret, described below. |

`GET /health` returns `200 {"status":"ok"}`. `GET /ready` returns
`200 {"status":"ready"}` only while admission is open, and
`503 {"status":"unready"}` while draining, under memory pressure, below the
cluster floor or at the WebSocket ceiling. Ingress-byte occupancy alone does not
make the node unready. `GET /metrics` is Prometheus text: readiness, drain,
connections/sessions, reroutes/rejections, ingress and in-flight bytes, pressured
sources, shedding/timeouts, frame and delivery histograms and fixed-dimension
handshake outcomes. Names, labels and buckets match the runtime-independent
Elixir families. Linux RSS replaces BEAM memory families. Forwarded counters
advance at emission, so receipt/timeout evidence is still needed for delivery.

SIGTERM/SIGINT stops admission, allows 30 seconds for existing sessions to drain,
then sends retryable `1012` closes and allows two seconds for socket cleanup.
Sessions retain a 30-second empty-owner grace; leases pin the owner across an
upgrade and idle expiry checks the exact generation. Fatal listener/cluster
failure closes existing sessions before service exit.

## Public protocol and resource boundaries

`/ws` retains v1 and v2 server/client roles, duplicate-role replacement, v2
sync/connected/disconnected/ping-pong control, generated client connection IDs,
per-client data routes and fanout. An unresponsive control is nudged after ten
seconds without client data attachment and reset five seconds later.
`serverId` and v2 `connectionId` are limited to 256 UTF-8 bytes. Malformed percent
escapes and invalid UTF-8 queries fail with 400 before route/admission state;
this deliberately avoids lossy route-key collisions, rather than accepting
arbitrary byte-string identifiers.

Maximum data payloads and reassembled messages are exactly `32 MiB - 14 bytes`,
matching the masked 32 MiB wire-frame contract. Control input is limited to
64 KiB. Oversized messages close with `1009`. Every matching client text/binary
`hello` or `e2ee_hello` frame requires a canonical padded Base64 supported
32-byte canonical X25519 key; a rejection closes with `1008` and is not forwarded.
Only small type/key strings decode; unrelated JSON metadata is scanned without
building a Value tree. Duplicate fields retain the original last-value behavior.
Other payloads remain byte-for-byte opaque.

Per-session state contains topology only. Independent Writers flush before
acknowledging a source, keeping reverse traffic available while source reads
pause. Each data Writer has one queue slot; queued data borrows the live charged
source, while further senders wait under their existing byte leases/deadlines.
Control notifications own bounded byte leases and cannot emit after expiry.
Cancelling an emitted write fences its destination; an unused queued reservation
may be skipped. A closing destination gets one second to flush its retryable
close before transport drop. Source last messages finish before normal detach.

The byte ledger starts at complete messages. Incomplete frame assembly remains
outside it; no strict pre-parser reservation is claimed. The RSS watermark pauses
admission, sheds blocked sources first then newest sockets, and remeasures every
100 ms between bounded batches. Recovery requires RSS below the watermark minus
one maximum payload. Enabled RSS-observation failure drains/closes the service.
Pre-upgrade HTTP slots, frame bounds, bounded decoding and measured RSS recovery
complement the weighted byte budget.

## Cluster contract

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

## Development and upstream maintenance

```sh
cargo fmt --manifest-path rust/Cargo.toml --check
cargo clippy --locked --manifest-path rust/Cargo.toml --all-targets -- -D warnings
cargo test --locked --manifest-path rust/Cargo.toml --all-targets
docker build -t paseo-relay-rust:local .
cargo test --locked --manifest-path rust/Cargo.toml --test binary -- --ignored
```

The build image pins Rust 1.97.1 and Cargo.lock pins dependencies. Tests use real
listeners, child processes, TCP partitions, DNS and containers. TDD.md records
red/green evidence and the exact remaining validation scope.

The Elixir reference at upstream baseline `5f4b763` keeps its original
`lib/`, `config/`, `test/`, mix.exs and mix.lock paths so ordinary rebases carry
upstream changes forward. Dockerfile.elixir builds it; scripts/ci.sh and the
reference CI job retain its original checks. With Elixir/OTP installed, run
`mix deps.get` and `mix test`; this is not required to deploy Rust.

| Upstream change | Rust implementation / verification |
| --- | --- |
| connection.ex / protocol.ex | connection.rs / protocol.rs, query and real-socket ceiling tests |
| handshake_validation.ex | protocol.rs, key/duplicate-field/memory-limit tests |
| ownership.ex / static_cluster.ex | session.rs / cluster.rs, real-process discovery/partition/restart tests |
| socket.ex / delivery/ / capacity.ex | relay.rs / session.rs / server.rs, pressure, cancellation and lifetime tests |
| metrics.ex / config.ex | metrics.rs / config.rs, public metric and environment tests |

For an upstream update, fetch and rebase the branch, inspect changes to these
boundaries, port behavior with a failing Rust regression first, then run both
reference and Rust gates and repeat comparative loads if a hot path changed.
Keep incoming Elixir Docker changes in Dockerfile.elixir when resolving the
intentional Dockerfile conflict; the default Dockerfile remains Rust. Public
protocol changes generally do not require changing the internal cluster format.

Comparisons use the same compiled WebSocket client for both images, validate each
payload/order, check server counters and final socket/byte cleanup, and retain
cgroup peaks, logs, source hashes and exact image IDs. Rebuild the baseline with
`docker build -f Dockerfile.elixir -t paseo-relay-elixir-baseline:5f4b763 .`, build
the Rust image as above, compile the load client using
`cargo build --locked --release --manifest-path rust/Cargo.toml --example relay_bench`,
and run the driver with `uv run python scripts/compare-rust.py --help`.

## Historical comparison

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
local paths. Those measured Rust snapshots predate distributed ownership and
complete metric parity; final comparisons of the current implementation remain
pending. The historical results establish lower memory
use and higher throughput for these workloads, not a universal latency or
open-loop capacity improvement. In particular, 64-pair 1 KiB p99 did not improve.
