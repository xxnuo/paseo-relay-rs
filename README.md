# Paseo Relay

A distributed, protocol-compatible relay for [Paseo](https://github.com/getpaseo/paseo),
implemented in Rust as one Linux executable. The default Dockerfile and image
publishing pipeline build a direct Rust scratch image without Nginx.
The Elixir implementation remains at its upstream source paths as a tested
compatibility and performance reference, not a runtime dependency. Frames remain end-to-end encrypted; the relay routes opaque payloads.

## Build and Linux deployment

```sh
scripts/ci-rust.sh
```

This Docker-only Rust pipeline checks formatting and Clippy, runs all native
and real-process tests inside the pinned Rust toolchain image, builds the
the Rust image, and runs real container lifecycle/cluster tests, including
61-second idle/resume, restart and live shutdown drain.
It needs Docker on Linux and access to its socket; no host Rust installation is
required. It extracts the static amd64 executable, common load client and
SHA256SUMS to `rust/target/docker/`. CI uses this same script and retains those
artifacts. The standalone binary needs neither Erlang nor Nginx:

```sh
PASEO_RELAY_HOST=127.0.0.1 PASEO_RELAY_PORT=4000 rust/target/docker/paseo-relay-linux-amd64
```

`deploy/paseo-relay.service` runs `/usr/local/bin/paseo-relay` as an unprivileged
systemd dynamic user, optionally reads `/etc/paseo-relay/environment`, and allows
40 seconds for shutdown. Set the host and memory watermark for the deployment.
The repository contains no provider inventories or proxy/operator configuration.

```sh
docker build -t paseo-relay-rust .
docker run --rm --stop-timeout 40 --ulimit nofile=65536:65536 -p 127.0.0.1:4000:4000 paseo-relay-rust
```

The default scratch image contains the static Rust executable only, runs as
UID/GID 65532 and listens directly on `PASEO_RELAY_HOST:PORT` (default
0.0.0.0:4000). Set Docker's file limit as above; the image contains no shell or
proxy supervisor to adjust it. SIGTERM drains the relay directly. Give Docker
at least 40 seconds to stop it. The explicit `binary` build target is equivalent
to the default `runtime` target. The existing GHCR workflow builds this default
direct image after both reference and Rust gates. Pin consumers to its reported
digest; checking out this branch does not change previously published images.
See [LICENSE](LICENSE).

## Self-managed VPS deployment target

Rust targets long-running nodes on independently purchased VPS hosts around the
world. Fly.io is the original deployment context supplied by the operator; Rust
does not depend on its edge routing, private network, autostart or TLS service.
Keep actual VPS addresses, credentials, certificates and provider inventories
outside this repository.

Use systemd's existing restart policy for the native binary, or Docker's
`--restart unless-stopped` for the image, with the documented 40-second stop
allowance. Terminate public TLS at the VPS ingress and expose a stable `wss://`
endpoint; the image currently supplies neither TLS nor automatic certificates. Keep `/metrics` and the cluster listener on management
or private-network paths. Authenticated cluster traffic is not encrypted: use
an encrypted private overlay between public VPS hosts rather than exposing its
control port globally.

For independent regional nodes, connect both roles of a session to the same
regional endpoint. Geography-based DNS alone can send the two roles to different
owners. A shared multi-region cluster requires an owner-aware ingress or client
that consumes `409 + x-reroute-target`: the relay returns that
response without Fly-style replay or automatic global forwarding.
A target is an opaque routing identity, not permission to proxy an arbitrary URL.
No automatic nearest-region selection or live cross-region socket migration is
claimed. These integration decisions belong to the consuming deployment.

Direct Rust is the measured starting point for low-cost VPS hosts. Set
`TOKIO_WORKER_THREADS` for actual resources. CPU quotas and worker counts from
the local comparison are not universal deployment recommendations.
Choose ingress/RSS limits with kernel and OS headroom. Current 2-second cluster
exchanges and 3-second membership expiry remain unchanged; assess regional
RTT/loss/partitions before changing them.

The planned Cloudflare integration is ordinary orange-cloud DNS proxying, not
Tunnel. [Cloudflare supports WebSockets](https://developers.cloudflare.com/network/websockets/)
but can terminate idle connections or restart edge servers; clients need
heartbeats and reconnect handling. Affinity for one client does not by itself
place both relay roles on the same owner. For
[Full (strict)](https://developers.cloudflare.com/ssl/origin-configuration/ssl-modes/full-strict/),
the origin must supply HTTPS with a matching valid certificate. Removing Nginx
does not implement Rust TLS; the current runtime remains plaintext and this
integration is not yet accepted. Public URLs must use a
[Cloudflare-supported port](https://developers.cloudflare.com/fundamentals/reference/network-ports/);
define the origin port mapping/routing separately for the local :4000 listener.
These local measurements exclude TLS, Cloudflare and WAN costs; do not infer
complete VPS sizing or a production RAM tier solely from them.

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
scripts/ci-rust.sh
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
the Rust image and load client with `scripts/ci-rust.sh`, and run
`uv run python scripts/compare-rust.py --bench rust/target/docker/relay_bench --help`.
Pass `--bench rust/target/docker/relay_bench` to use that Docker-built client in
every comparative run. Both implementations share the same total CPU/memory
quota. Earlier proxy comparison reports remain archived separately.

## Real-client scenario and stress tests

```sh
uv run python scripts/test-real-clients.py benchmarks/real-clients-run --clients 64 --seconds 60 --netem
```

This separate automated suite starts the relay in Docker (1 CPU, 512 MiB),
starts real Paseo daemons in temporary homes, and uses production client,
protocol and crypto TypeScript source from `../paseo`. Override with
`--paseo-source /path/to/paseo`. It requires Node 24, uv, local Linux Docker with
cgroup v2, and that checkout's existing workspace dependencies. The runner does
not install/build in or modify the reference checkout. Imports are redirected
to current source; required core modules, source hashes, reference HEAD and
unchanged Git status are verified, rejecting stale client/protocol/crypto dist.

Scenarios cover v2 control/data attachment, a client arriving before its daemon,
protected/legacy encryption policy, bad credentials/key, replay/ciphertext corruption,
concurrent RPCs and byte-verified file upload/download, a real PTY, production
heartbeats, a paused mobile reader, reconnect churn, relay restart and daemon
restart with retained identity. Browser-style sockets use Node's standard
WebSocket API; native/CLI-style sockets use the existing ws implementation with
the same production SDK. No relay, secure-channel or daemon application mock
is used. Providers/plugins are disabled; tests do not invoke a paid model.

`--seconds` controls the mixed-RPC load phase, not the total suite duration.
`--netem` builds a test-only tc helper and requires NET_ADMIN in its container;
100-ms egress delay, 10-ms jitter and 0.5% loss are applied only to the relay's
network namespace, then removed. Omit it when that capability is unavailable;
the report will not claim network impairment coverage. `--no-build --image TAG`
tests an already built compatible image, including Dockerfile.elixir's image.
The ordinary Rust CI gate remains self-contained and does not silently count
this external-source integration suite as passed.

Each unique output directory retains per-phase results, file SHA256 checks,
client/daemon source import hashes, encrypted-wire counters, relay metrics,
cgroup samples and Docker OOM events across restart, plus driver CPU/RSS/event
loop lag. Failure/timeout is nonzero and owned containers/temp homes are cleaned.
These are real local stacks plus optional simulated network impairment, not
Expo/Electron UI, model-provider chat, TLS, Cloudflare or global VPS acceptance.
The Node driver also hosts the daemons, so its CPU/event loop can limit offered
load; this is not a maximum relay-capacity benchmark. Run it on a sufficiently
provisioned test host: the recorded 64-client driver used about 1.14 GiB outside
the relay quota. Two 64-client/60-second runs passed all 16 scenarios on the
1-CPU/512-MiB relay, with peak relay cgroup memory about 70.6 MiB. The original
Elixir image also passed the smaller compatibility suite.
[Acceptance report and raw evidence](benchmarks/README.md) record the results
and measurement boundaries. Legacy here means the shared encryption mode;
the retired Cloudflare Durable Object adapter is not used.

## Performance decision and history

Nginx support was removed after the 46-run relay-cost comparison: direct Rust
used less memory and less CPU at matched traffic, and delivered higher saturated
throughput. The source-level removal also deletes the optional proxy image and
its worker/configuration/supervisor paths. The tested direct runtime is retained.

[Decision, work log and historical comparisons](benchmarks/README.md) preserve
the rationale, source checkpoints, measurement limits and raw evidence links.
[TDD.md](TDD.md) records red/green checks and final acceptance. Historical proxy
experiments must use their recorded source snapshots, not the current build.
