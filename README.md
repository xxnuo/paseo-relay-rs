# Paseo Relay

A distributed, protocol-compatible relay for [Paseo](https://github.com/getpaseo/paseo),
implemented in Rust as one Linux executable. The default Dockerfile and image
publishing pipeline build a direct Rust scratch image. Nginx is optional.
The Elixir implementation remains at its upstream source paths as a tested
compatibility and performance reference, not a runtime dependency. Frames remain end-to-end encrypted; the relay routes opaque payloads.

## Build and Linux deployment

```sh
scripts/ci-rust.sh
```

This Docker-only Rust pipeline checks formatting and Clippy, runs all native
and real-process tests inside the pinned Rust toolchain image, builds the
direct Rust and optional Nginx images, and runs real container lifecycle/cluster
tests. Both paths include a 61-second idle/resume and restart test.
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

### Optional Nginx image

```sh
docker build --target nginx -t paseo-relay-nginx .
```

This optional target retains pinned Nginx 1.30.6, UID/GID 65532 and the tested
supervisor. Nginx owns the configured public host/port; Rust uses loopback
:4001, or :4002 when the public port is 4001. The independent cluster port is
unchanged. The entrypoint raises the file limit to 65536, so its hard limit
must allow that. Each proxy worker has 65536 connection slots; both sides of a
proxied connection consume slots/descriptors. Proxy buffering is disabled.
WebSocket inactivity is limited to 24 hours; HTTP timeouts follow
`HTTP_IDLE_TIMEOUT_MS`. Backend HTTP slots do not bound all Nginx front-end
connections, and Rust's RSS watermark excludes proxy/kernel memory. SIGTERM
first drains Rust, then stops Nginx; either service failing stops the sibling.
This target serves plain HTTP/WebSocket unless deployment-specific TLS is added.

## Self-managed VPS deployment target

Rust targets long-running nodes on independently purchased VPS hosts around the
world. Fly.io is the original deployment context supplied by the operator; Rust
does not depend on its edge routing, private network, autostart or TLS service.
Keep actual VPS addresses, credentials, certificates and provider inventories
outside this repository.

Use systemd's existing restart policy for the native binary, or Docker's
`--restart unless-stopped` for the image, with the documented 40-second stop
allowance. Terminate public TLS at the VPS ingress and expose a stable `wss://`
endpoint; neither image currently supplies TLS or automatic certificates. Keep `/metrics` and the cluster listener on management
or private-network paths. Authenticated cluster traffic is not encrypted: use
an encrypted private overlay between public VPS hosts rather than exposing its
control port globally.

For independent regional nodes, connect both roles of a session to the same
regional endpoint. Geography-based DNS alone can send the two roles to different
owners. A shared multi-region cluster requires an owner-aware ingress or client
that consumes `409 + x-reroute-target`: direct Rust and the optional proxy pass
that response through, without Fly-style replay or automatic global forwarding.
A target is an opaque routing identity, not permission to proxy an arbitrary URL.
No automatic nearest-region selection or live cross-region socket migration is
claimed. These integration decisions belong to the consuming deployment.

Direct Rust is the measured starting point for low-cost VPS hosts. Set
`TOKIO_WORKER_THREADS` for actual resources; the optional Nginx target defaults
to one worker and accepts `PASEO_RELAY_NGINX_WORKERS` (1–64). CPU quotas/worker
counts from the local comparison are not universal deployment recommendations.
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
does not implement Rust TLS; both current targets remain plaintext and this
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
| `NGINX_WORKERS` | `1` | Optional nginx target only, 1–64; ignored by direct Rust. |
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
quota; the Rust container includes all Nginx workers in that quota.

## Direct Rust versus Nginx: relay cost decision

The default image now omits Nginx. The relay workload did not need the extra
proxy to preserve connection lifetime, route opaque traffic or enforce the
application limits. Its removal reduced measured memory/CPU cost and increased
saturated throughput. The optional `nginx` target remains available for a
deployment that needs separate proxy features. Origin TLS for ordinary
Cloudflare DNS proxying still needs implementation/configuration; this decision
does not claim a complete Cloudflare-ready HTTPS deployment.

Both images contain byte-identical Rust executables. The Nginx image uses the
previously optimized single worker. Measurements used isolated 1-GiB containers,
2 CPUs/two Tokio workers except the explicit 1-CPU/one-worker row, the same
Docker-built external client with four workers, loopback port mappings and
alternating order. Rows below are medians of three runs per image. Memory is
cgroup container accounting, including proxy processes/kernel allocations; it
is not complete VPS RAM usage. Idle rows sample ten seconds after warmup with
no application traffic, then prove both directions still work. They do not
include production heartbeat traffic.

| Idle connections | Memory MiB, Nginx / direct | CPU % of one core, Nginx / direct |
| --- | ---: | ---: |
| 2 | 31.5 / 1.2 | 0.598 / 0.045 |
| 1024 | 211.6 / 148.3 | 0.594 / 0.071 |

The 1024-connection idle sample saves about 63 MiB (30%). Absolute idle CPU is
low in both modes. To compare CPU under the same business volume, a separate
paced test sends 200 frames/s per pair and asserts the achieved rate within 2%.
CPU below is container CPU time divided by the full observed wall window,
including setup/warmup and cleanup; 100% means one CPU core. The traffic rate is
the measured data phase. Each load phase lasts ten seconds.

| Matched traffic | Frames/s, Nginx / direct | CPU % of one core, Nginx / direct | p99 ms, Nginx / direct |
| --- | ---: | ---: | ---: |
| 64 pairs, 1 KiB | 12,803 / 12,802 | 53.2 / 38.7 | 2.490 / 2.563 |
| 16 pairs, 64 KiB | 3,202 / 3,202 | 45.2 / 22.4 | 3.271 / 1.244 |

At matched traffic, CPU falls 27% and 51%. The small-frame paced p99 increases
slightly by 0.073 ms; no claim is made that every latency metric improves.
Maximum-throughput tests use ten seconds after 20 warmup exchanges per pair:

| Saturated workload | Frames/s, Nginx / direct | p99 ms, Nginx / direct | Peak MiB, Nginx / direct |
| --- | ---: | ---: | ---: |
| 2 CPUs, 64 pairs, 1 KiB | 55,847 / 76,779 | 7.856 / 2.324 | 57.3 / 22.3 |
| 2 CPUs, 64 pairs, 64 KiB | 8,376 / 15,270 | 10.860 / 7.371 | 72.5 / 35.3 |
| 1 CPU, 64 pairs, 1 KiB | 35,566 / 51,460 | 25.914 / 3.367 | 56.9 / 21.8 |

A further single 30-second v2 4-KiB run and single five-second v1 run per image
also delivered every frame and recovered socket/byte reservations. These are
smoke/sustained-local observations, not production soak tests. All 46 runs
passed payload/order, exact counters including warmup, resource cleanup and
zero OOM events. Both images passed 61-second idle/resume/restart tests; direct
Rust separately passed real-container ownership/reroute/replay tests.
[Summary](benchmarks/proxy-decision-summary.json) indexes the raw evidence,
CPU windows, image IDs and source/client fingerprints. Driver snapshots are
`aa26cc2` (idle/saturation) and `4003e45` (matched rate).

For repeatability, build both images and the client with `scripts/ci-rust.sh`.
Use `scripts/compare-rust.py` with `--elixir-image` set to the Nginx image and
`--rust-image` to the direct image, `--baseline-label nginx --candidate-label
direct`, and `--bench rust/target/docker/relay_bench`. `--idle` measures established
idle sockets; `--paced` measures matched traffic; otherwise it measures saturated
throughput. `--cpus 1 --workers 1` selects the cheap-VPS case. Historical image
option names remain compatible with earlier original-versus-Rust comparisons.

Cloudflare, origin TLS encryption/handshakes, WAN loss and real VPS pricing are
outside these tests. Removing proxy overhead creates headroom; it does not by
itself prove that a lower RAM-priced VPS tier can handle maximum messages and
the deployment's real connection/traffic profile. Current plaintext images
must not be mistaken for a Full (strict) origin.

## Historical VPS proxy sizing check

The then-default one-worker image at `a8cac64` was checked against the prior two-worker
image using byte-identical Rust executables and the same Docker-built load
client. Each container had 2 CPUs/1 GiB, Tokio used two workers, and v2 had
64 pairs, 20 warmup exchanges and 10 measured seconds. Medians below are from
three alternating runs per configuration; this is a local sizing experiment.

| Payload | Frames/s, 2 / 1 workers | p99 ms, 2 / 1 workers | Peak container MiB, 2 / 1 workers |
| --- | ---: | ---: | ---: |
| 1 KiB | 52,777 / 56,290 | 23.285 / 7.543 | 85.2 / 57.5 |
| 64 KiB | 7,653 / 8,375 | 37.772 / 10.847 | 102.4 / 72.7 |

All 12 runs passed content/order, server counters and socket/byte cleanup, with
zero OOM events. The 64 KiB two-worker throughput varied from 6,037 to 8,641
frames/s, so its median throughput difference is not a robust capacity claim.
The lower fixed footprint and p99 support a one-worker starting point for
small VPS hosts; size larger hosts from their own measurements. Full records,
image IDs, equal executable hashes and ranges are in
[the sizing evidence](benchmarks/vps-worker-comparison/summary.json). This does
not establish WAN latency or replace the earlier original-versus-Rust test.

## Original Docker versus Rust + Nginx Docker (two-worker snapshot)

This historical two-worker snapshot predates the configurable one-worker VPS
default described above. The Docker-only build at `a5df513` was compared with the original Elixir image
rebuilt from the unchanged `5f4b763` source/Dockerfile. The same Docker-built
static load client checked both implementations. Each fresh container had a
2-CPU quota and 1 GiB memory limit; Elixir and Tokio each used two runtime
workers. Rust's quota and memory measurements include its Nginx master, two
Nginx workers and entrypoint. The external load client used four workers.

These are loopback Docker port mappings, closed-loop bidirectional exchanges,
20 warmup exchanges per pair, three runs per implementation with alternating
order, and 10 measured seconds per run (three for the maximum payload).
Latency is from sending a frame to receipt at the other endpoint. The table
reports medians; maximum-message runs contain only a few dozen measured frames,
so their p99 is particularly sensitive to scheduling.

| Workload | Original frames/s | Rust + Nginx frames/s | Throughput ratio | p99 ms, original / Rust + Nginx | Peak container MiB, original / Rust + Nginx |
| --- | ---: | ---: | ---: | ---: | ---: |
| v2, 1 pair, 1 KiB | 23,943 | 33,016 | 1.38 | 0.083 / 0.054 | 115.6 / 61.2 |
| v2, 64 pairs, 1 KiB | 36,830 | 51,628 | 1.40 | 3.006 / 23.942 | 151.1 / 85.1 |
| v2, 64 pairs, 64 KiB | 10,061 | 8,401 | 0.84 | 10.636 / 38.355 | 231.9 / 102.3 |
| v1, 64 pairs, 1 KiB | 38,017 | 54,181 | 1.43 | 2.741 / 23.421 | 150.1 / 85.3 |
| v2, 1 pair, 32 MiB − 14 bytes | 10.5 | 9.8 | 0.93 | 102.804 / 128.201 | 563.7 / 194.0 |

For these workloads, small-message throughput is 38–43% higher, while 64 KiB
throughput is 16.5% lower and maximum-message throughput is 6.6% lower. Peak
container memory is 43–66% lower. Concurrent p99 is substantially worse with
Nginx. CPU quota throttling is directly recorded: for v2 64-pair 1 KiB,
Rust + Nginx has a median 101 throttled periods out of 102 observed periods.
This coincides with the latency regression; it is not proof that quota
throttling alone explains every difference. No CPU quota was increased to hide
proxy overhead. This image is not a universal performance improvement.

All 30 runs passed exact payload/order checks, matching server frame/byte
counters including warmup, final socket/ingress cleanup and zero OOM events.
Every Rust process inventory confirms the relay, Nginx master and two workers.
[Summary and CPU observations](benchmarks/nginx-summary.json) link the raw load,
metrics and cgroup records. Source/image/load-client fingerprints were verified
against the frozen checkout; `source_dirty` is false in every dataset. WAN,
open-loop capacity, cluster scaling and long-duration reliability were not
measured. The direct-Rust results below are historical and exclude
Nginx, so they must not be substituted for this table.

Docker publication was also exercised with a temporary local registry: push,
pull by immutable digest, Nginx-backed readiness and graceful stop all passed.
[Publication evidence](benchmarks/nginx-validation/publication.json) records the
digest and image ID. No remote registry or Git push was performed; the workflow at that snapshot built the default Nginx image. The current
default is direct Rust as described above.

## Historical direct-Rust performance comparison

The complete Rust implementation at `90e98b0` and retained Elixir baseline
`5f4b763` were measured on the same Linux amd64 host through loopback Docker
port mappings. Each fresh container had a 2-CPU quota, 1 GiB memory limit and
two runtime workers; the external client used four workers. Each pair warmed
up for 20 bidirectional exchanges, then ran for 10 seconds (three seconds for
the maximum-size payload). Each row reports medians from three runs per
implementation, alternating execution order. Latency measures sending a frame
until the other endpoint receives it, not an application round trip.

| Workload | Elixir frames/s | Rust frames/s | Rust throughput ratio | p99 ms, Elixir / Rust | Peak container MiB, Elixir / Rust |
| --- | ---: | ---: | ---: | ---: | ---: |
| v2, 1 pair, 1 KiB | 25,148 | 48,290 | 1.92 | 0.064 / 0.031 | 130.2 / 7.6 |
| v2, 64 pairs, 1 KiB | 38,208 | 78,903 | 2.07 | 2.808 / 2.287 | 156.7 / 22.3 |
| v2, 64 pairs, 64 KiB | 10,423 | 16,199 | 1.55 | 9.796 / 6.782 | 240.7 / 35.3 |
| v1, 64 pairs, 1 KiB | 38,749 | 77,911 | 2.01 | 2.725 / 2.351 | 159.4 / 22.5 |
| v2, 1 pair, 32 MiB − 14 bytes | 12 | 19 | 1.59 | 95.233 / 62.195 | 601.6 / 134.9 |

All 30 runs passed exact payload/order checks. Server frame/byte counters matched
including warmup, WebSockets and ingress leases returned to zero, and OOM events
were zero. [Final summary](benchmarks/final-summary.json) indexes the datasets;
each retains load output, metrics, cgroup CPU/memory observations, image IDs and
source/load-generator SHA256 fingerprints. All source fingerprints were verified
against the committed implementation; `source_dirty` is false in every dataset.
The extracted static executable fingerprint and validation gates are recorded in
[delivery evidence](benchmarks/final-validation/delivery.json).

These are closed-loop, single-outstanding-message measurements of local routing.
They show 1.55–2.07 times the throughput, lower p99 and 77.6–94.1% lower peak
container memory for these five workloads. They do not establish open-loop
capacity, WAN latency, cluster-gossip scaling or long-duration production
reliability. The private cluster is functionally tested separately. Earlier
pre-cluster results remain in `benchmarks/2026-11-01-summary.json` as historical
evidence and are not the basis of this table.
