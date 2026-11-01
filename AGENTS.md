# Paseo Relay — Agent Guide

Paseo Relay is a Rust WebSocket relay built as one Linux executable. Tokio/Axum
serve sockets; per-serverId sessions and authenticated cluster ownership keep
frames on one node. Frames are end-to-end encrypted. The Elixir/OTP application
in lib/config/test is retained as the upstream compatibility/performance reference.

This repository owns the application, tests, generic Dockerfile, image publishing
and documented runtime contract. Consumers integrate through the GHCR image
pinned by digest and the environment settings in README.md. Keep provider
configuration, inventories, proxies and operator procedures outside this repo.

## Development

```sh
scripts/ci-rust.sh
```

## Conventions

- Rust targets long-running, self-managed VPS nodes across multiple regions.
  Fly.io is the original deployment context, not a Rust runtime dependency.
  Do not assume Fly replay/routing, private DNS, autostart or automatic TLS.
  Keep provider inventories outside this repo. Tune per VPS resources and
  measured WAN behavior; loopback Docker results are not global acceptance.
- Platform-agnostic core: no provider dependencies in application or scripts.
- Use real TCP listeners, WebSockets, Rust child processes and Docker/DNS,
  without mocks. The retained Elixir tests use real Cowboy/:peer BEAM nodes.
- Keep reference source paths stable for upstream rebases. Dockerfile.elixir
  builds that baseline; Dockerfile builds direct Rust only. Nginx support was
  removed; see benchmarks/README.md for the decision and historical evidence.
  Planned Cloudflare ingress uses orange-cloud DNS proxying, not Tunnel; origin
  TLS must be handled explicitly. The test
  target runs Rust gates in Docker; scripts/ci-rust.sh also tests the final image
  and extracts static artifacts. scripts/ci.sh verifies Elixir.
- Every behavior change needs a red test first and evidence in TDD.md.
- Fail closed on session, Writer, capacity and cluster authority loss; never
  release charged payload memory while a queue can still retain it.
- Capacity limits and timeouts belong to the README runtime contract; document
  changes there in the same commit.
- Never dump full process state on live nodes or combine production actions.
