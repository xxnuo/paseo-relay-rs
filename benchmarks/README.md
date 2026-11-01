# Relay performance decisions and work log

## 2026-11-01: remove Nginx support completely

The operator requested removal, including the optional image. Rust serves
long-lived opaque relay WebSockets on self-managed VPS hosts. Ordinary
Cloudflare orange-cloud DNS proxying is planned; Tunnel is not the target.
A second local relay proxy added memory, CPU, sockets and a supervisor without
improving the required forwarding behavior.

The 46-run comparison below used byte-identical Rust executables, a previously
optimized one-worker Nginx, alternating order, exact payload/order checks and
matching counters/cleanup. At 1024 idle sockets, direct memory was 148.3 MiB
versus 211.6 MiB. At matched traffic, direct CPU was 27–51% lower. Saturated
throughput increased 37–82% in the two tested two-CPU workloads. The small-frame
paced p99 increased only 0.073 ms; this tradeoff is preserved below.

Chronology and source checkpoints:

- `a5df513`: packaged Rust with Nginx; Docker-only build, tests and local registry
  publication checks. The original Docker comparison exposed proxy overhead.
- `a8cac64`, `06b3955`: made proxy workers configurable, chose one worker for
  small VPS hosts, and recorded the two-versus-one-worker experiment.
- `aa26cc2`, `4003e45`: added idle and matched-rate CPU measurements, with both
  source snapshots and both Docker-built driver fingerprints retained.
- `676bd1b`: selected direct Rust by default from the 46-run comparison, while
  temporarily retaining the optional proxy image.
- This removal deletes the proxy build stage, config, supervisor, worker
  setting, optional CI image and proxy-specific tests. Direct idle/restart,
  single-process/nonroot checks, live drain and cluster tests remain. The
  generic comparison driver continues to compare actual supplied images.

Current Docker builds and publication use Rust only. Historical reports,
metrics, logs, image identities and source hashes remain unchanged. Engineering
red/green records and the removal acceptance result are in [TDD.md](../TDD.md).
No new performance claim is made for deleting an unused build target; the
previous direct runtime is retained.

Both measured paths were plaintext loopback traffic. Cloudflare origin TLS,
WAN, production heartbeat traffic and real VPS price tiers were not measured.
Removing Nginx does not implement native TLS or make the image a Full (strict)
origin. This remains a separate deployment requirement.

## Archived comparison narratives

The sections below were moved from README.md at `676bd1b`. They describe the
capabilities and defaults of their recorded historical snapshots, including
Nginx support that no longer exists. To reproduce a proxy experiment, use its
recorded commit in an isolated checkout and its exact image/client fingerprints;
current builds deliberately do not offer that target. Dataset links below are
relative to this benchmarks directory.

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
[Summary](proxy-decision-summary.json) indexes the raw evidence,
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
[the sizing evidence](vps-worker-comparison/summary.json). This does
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
[Summary and CPU observations](nginx-summary.json) link the raw load,
metrics and cgroup records. Source/image/load-client fingerprints were verified
against the frozen checkout; `source_dirty` is false in every dataset. WAN,
open-loop capacity, cluster scaling and long-duration reliability were not
measured. The direct-Rust results below are historical and exclude
Nginx, so they must not be substituted for this table.

Docker publication was also exercised with a temporary local registry: push,
pull by immutable digest, Nginx-backed readiness and graceful stop all passed.
[Publication evidence](nginx-validation/publication.json) records the
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
were zero. [Final summary](final-summary.json) indexes the datasets;
each retains load output, metrics, cgroup CPU/memory observations, image IDs and
source/load-generator SHA256 fingerprints. All source fingerprints were verified
against the committed implementation; `source_dirty` is false in every dataset.
The extracted static executable fingerprint and validation gates are recorded in
[delivery evidence](final-validation/delivery.json).

These are closed-loop, single-outstanding-message measurements of local routing.
They show 1.55–2.07 times the throughput, lower p99 and 77.6–94.1% lower peak
container memory for these five workloads. They do not establish open-loop
capacity, WAN latency, cluster-gossip scaling or long-duration production
reliability. The private cluster is functionally tested separately. Earlier
pre-cluster results remain in `benchmarks/2026-11-01-summary.json` as historical
evidence and are not the basis of this table.
