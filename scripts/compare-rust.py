#!/usr/bin/env -S uv run python
"""Compare isolated relay containers using one checked real-WebSocket workload."""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import re
import subprocess
import time
import urllib.request


def command(*args):
    return subprocess.check_output(args, text=True).strip()


def get(url):
    with urllib.request.urlopen(url, timeout=3) as response:
        return response.read().decode()


def gauges(metrics):
    return dict(re.findall(r"^(paseo_relay_\w+) (\d+)$", metrics, re.M))


def cgroup(pid):
    entry = next(line[3:] for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines() if line.startswith("0::"))
    root = Path("/sys/fs/cgroup") / entry.lstrip("/")
    return {name: (root / name).read_text() for name in ("cpu.stat", "memory.current", "memory.peak", "memory.events")}


def run(args, image, label, directory):
    directory.mkdir()
    cid = command("docker", "run", "--detach", "--cpus=2", "--memory=1g", "--pids-limit=512", "--ulimit=nofile=65536:65536", "--publish", "127.0.0.1::4000", "--env", "PASEO_RELAY_HOST=0.0.0.0", "--env", "ERL_FLAGS=+S 2:2 +A 2", "--env", "TOKIO_WORKER_THREADS=2", image)
    try:
        inspection = json.loads(command("docker", "inspect", cid))[0]
        port = inspection["NetworkSettings"]["Ports"]["4000/tcp"][0]["HostPort"]
        base = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 15
        while True:
            try:
                assert json.loads(get(base + "/ready")) == {"status": "ready"}
                break
            except (OSError, AssertionError):
                if time.monotonic() >= deadline:
                    raise RuntimeError("relay did not become ready")
                time.sleep(0.05)
        before_metrics = get(base + "/metrics")
        (directory / "metrics-before.txt").write_text(before_metrics)
        before = cgroup(inspection["State"]["Pid"])
        completed = subprocess.run([str(args.bench), f"127.0.0.1:{port}", str(args.pairs), str(args.seconds), str(args.payload_bytes), args.protocol], env={**os.environ, "TOKIO_WORKER_THREADS": "4"}, text=True, capture_output=True, timeout=args.seconds+90)
        (directory / "load.stdout").write_text(completed.stdout)
        (directory / "load.stderr").write_text(completed.stderr[:20000])
        completed.check_returncode()
        load = json.loads(completed.stdout)
        assert load["measured_seconds"] >= args.seconds
        assert load["loss"] == load["ordering_errors"] == load["corruption"] == 0
        deadline = time.monotonic() + 5
        while True:
            after_metrics = get(base + "/metrics")
            after_gauges = gauges(after_metrics)
            if int(after_gauges["paseo_relay_active_websockets"]) == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("relay did not clean up WebSockets")
            time.sleep(0.01)
        assert int(after_gauges["paseo_relay_ingress_reserved_bytes"]) == 0
        expected_frames = load["forwarded_frames"] + 40*args.pairs
        before_gauges = gauges(before_metrics)
        assert int(after_gauges["paseo_relay_frames_forwarded_total"]) - int(before_gauges["paseo_relay_frames_forwarded_total"]) == expected_frames
        assert int(after_gauges["paseo_relay_bytes_forwarded_total"]) - int(before_gauges["paseo_relay_bytes_forwarded_total"]) == expected_frames * args.payload_bytes
        after = cgroup(inspection["State"]["Pid"])
        (directory / "metrics-after.txt").write_text(after_metrics)
        result = {"implementation": label, "image": image, "image_id": inspection["Image"], "limits": {"cpus": 2, "memory_bytes": 1073741824, "worker_threads": 2}, "load": load, "cgroup_before": before, "cgroup_after": after}
        (directory / "result.json").write_text(json.dumps(result, indent=2)+"\n")
        print(json.dumps({"run": directory.name, "fps": round(load["frames_per_second"]), "p99_us": load["latency_us"]["p99"], "peak_bytes": int(after["memory.peak"])}), flush=True)
        return result
    finally:
        logs = subprocess.run(["docker", "logs", cid], capture_output=True, text=True)
        (directory / "relay.log").write_text((logs.stdout + logs.stderr)[:20000])
        subprocess.run(["docker", "rm", "--force", cid], check=True, stdout=subprocess.DEVNULL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--bench", type=Path, default=Path("rust/target/release/examples/relay_bench"))
    parser.add_argument("--elixir-image", default="paseo-relay-elixir-baseline:5f4b763")
    parser.add_argument("--rust-image", default="paseo-relay-rust:local")
    parser.add_argument("--pairs", type=int, default=64)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--protocol", choices=["v1", "v2"], default="v2")
    args = parser.parse_args()
    if not (1 <= args.pairs <= 20000 and 1 <= args.seconds <= 3600 and 16 <= args.payload_bytes <= 33554418 and 1 <= args.repetitions <= 20):
        parser.error("invalid workload bounds")
    args.output.mkdir(parents=True, exist_ok=False)
    results = []
    metadata = {"git_commit": command("git", "rev-parse", "HEAD"), "dirty": bool(command("git", "status", "--porcelain")), "host": platform.platform(), "cpu": next(line.split(":",1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "workload": {"pairs": args.pairs, "seconds": args.seconds, "payload_bytes": args.payload_bytes, "protocol": args.protocol}}
    sources = [Path("rust/Cargo.toml"), Path("rust/Cargo.lock"), Path("Dockerfile"), Path("Dockerfile.elixir"), Path("rust/examples/relay_bench.rs"), Path(__file__)] + sorted(Path("rust/src").glob("*.rs"))
    metadata["source_dirty"] = bool(command("git", "status", "--porcelain", "--", *(str(path) for path in sources)))
    metadata["source_sha256"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    metadata["bench_binary_sha256"] = hashlib.sha256(args.bench.read_bytes()).hexdigest()
    (args.output / "metadata.json").write_text(json.dumps(metadata,indent=2)+"\n")
    for repetition in range(args.repetitions):
        order = [("elixir", args.elixir_image), ("rust", args.rust_image)]
        if repetition % 2:
            order.reverse()
        for label, image in order:
            results.append(run(args, image, label, args.output / f"{repetition+1}-{label}"))
            (args.output / "results.json").write_text(json.dumps(results,indent=2)+"\n")


if __name__ == "__main__":
    main()
