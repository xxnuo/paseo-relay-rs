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

- Platform-agnostic core: no provider dependencies in application or scripts.
- Use real TCP listeners, WebSockets, Rust child processes and Docker/DNS,
  without mocks. The retained Elixir tests use real Cowboy/:peer BEAM nodes.
- Keep reference source paths stable for upstream rebases. Dockerfile.elixir
  builds that baseline; Dockerfile builds Rust + Nginx by default. Its test
  target runs Rust gates in Docker; scripts/ci-rust.sh also tests the final image
  and extracts static artifacts. scripts/ci.sh verifies Elixir.
- Every behavior change needs a red test first and evidence in TDD.md.
- Fail closed on session, Writer, capacity and cluster authority loss; never
  release charged payload memory while a queue can still retain it.
- Capacity limits and timeouts belong to the README runtime contract; document
  changes there in the same commit.
- Never dump full process state on live nodes or combine production actions.
