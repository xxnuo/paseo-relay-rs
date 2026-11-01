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
# The test container reaches the sibling relay containers via host-mapped ports.
docker run --rm --network host \
  --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock \
  --env "PASEO_RELAY_TEST_IMAGE=$runtime_image" "$test_image"

case "$(docker info --format '{{.Architecture}}')" in
  x86_64|amd64) readonly arch=amd64 ;;
  aarch64|arm64) readonly arch=arm64 ;;
  *) echo 'Unsupported Docker architecture' >&2; exit 1 ;;
esac
mkdir -p rust/target/docker
runtime_container=$(docker create "$runtime_image")
test_container=$(docker create "$test_image")
docker cp "$runtime_container:/paseo-relay" rust/target/docker/paseo-relay-linux-"$arch"
docker cp "$test_container:/app/target/release/examples/relay_bench" rust/target/docker/relay_bench
(cd rust/target/docker && sha256sum "paseo-relay-linux-$arch" relay_bench > SHA256SUMS)
