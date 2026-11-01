#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

readonly runtime_image="${PASEO_RELAY_TEST_IMAGE:-paseo-relay-rust:local}"
readonly test_image="${runtime_image}-test"
runtime_container=""
test_container=""
cleanup() {
  if [[ -n "$runtime_container" ]]; then docker rm -f "$runtime_container" >/dev/null; fi
  if [[ -n "$test_container" ]]; then docker rm -f "$test_container" >/dev/null; fi
}
trap cleanup EXIT

docker build --target test --tag "$test_image" .
docker build --tag "$runtime_image" .
docker build --target binary --tag "${runtime_image}-direct" .
# The test container reaches the sibling relay containers via host-mapped ports.
docker run --rm --network host \
  --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \
  --env "PASEO_RELAY_TEST_IMAGE=$runtime_image" \
  --env "PASEO_RELAY_DIRECT_TEST_IMAGE=${runtime_image}-direct" "$test_image"

mkdir -p rust/target/docker
runtime_container=$(docker create "$runtime_image")
test_container=$(docker create "$test_image")
docker cp "$runtime_container:/paseo-relay" rust/target/docker/paseo-relay-linux-amd64
docker cp "$test_container:/app/target/release/examples/relay_bench" rust/target/docker/relay_bench
(cd rust/target/docker && sha256sum paseo-relay-linux-amd64 relay_bench > SHA256SUMS)
