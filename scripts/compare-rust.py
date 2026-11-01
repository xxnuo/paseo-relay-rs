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
    cpus = getattr(args, "cpus", 2)
    workers = getattr(args, "workers", 2)
    cid = command("docker", "run", "--detach", f"--cpus={cpus}", "--memory=1g", "--pids-limit=512", "--ulimit=nofile=65536:65536", "--publish", "127.0.0.1::4000", "--env", "PASEO_RELAY_HOST=0.0.0.0", "--env", f"ERL_FLAGS=+S {workers}:{workers} +A 2", "--env", f"TOKIO_WORKER_THREADS={workers}", image)
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
        observation_started = time.monotonic()
        before = cgroup(inspection["State"]["Pid"])
        idle = getattr(args, "idle", False)
        load_args = [str(args.bench), f"127.0.0.1:{port}", str(args.pairs), str(args.seconds + 5 if idle else args.seconds), str(args.payload_bytes), args.protocol]
        if idle:
            load_args.append("idle")
        if getattr(args, "paced", False):
            load_args.append("paced")
        idle_observation = None
        with subprocess.Popen(load_args, env={**os.environ, "TOKIO_WORKER_THREADS": "4"}, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
            try:
                if idle:
                    deadline = time.monotonic() + 60
                    while True:
                        current = gauges(get(base + "/metrics"))
                        if int(current["paseo_relay_frames_forwarded_total"]) == 40 * args.pairs and int(current["paseo_relay_active_websockets"]) == 2 * args.pairs:
                            break
                        if process.poll() is not None or time.monotonic() >= deadline:
                            raise RuntimeError("idle connections did not finish warmup")
                        time.sleep(0.05)
                    idle_before = cgroup(inspection["State"]["Pid"])
                    started = time.monotonic()
                    time.sleep(args.seconds)
                    idle_after = cgroup(inspection["State"]["Pid"])
                    current = gauges(get(base + "/metrics"))
                    assert int(current["paseo_relay_frames_forwarded_total"]) == 40 * args.pairs
                    assert int(current["paseo_relay_active_websockets"]) == 2 * args.pairs
                    idle_observation = {"seconds": time.monotonic() - started, "cgroup_before": idle_before, "cgroup_after": idle_after, "active_websockets": 2 * args.pairs}
                stdout, stderr = process.communicate(timeout=args.seconds+90)
                completed = subprocess.CompletedProcess(load_args, process.returncode, stdout, stderr)
            except BaseException:
                process.kill()
                process.communicate()
                raise
        (directory / "load.stdout").write_text(completed.stdout)
        (directory / "load.stderr").write_text(completed.stderr[:20000])
        completed.check_returncode()
        load = json.loads(completed.stdout)
        assert load["measured_seconds"] >= args.seconds
        assert load["loss"] == load["ordering_errors"] == load["corruption"] == 0
        if idle:
            assert load["idle"] and load["forwarded_frames"] == 2 * args.pairs
        if getattr(args, "paced", False):
            assert load["paced"] and 0.98 <= load["frames_per_second"] / (200 * args.pairs) <= 1.02, "fixed traffic rate was not sustained"
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
        observation_seconds = time.monotonic() - observation_started
        (directory / "metrics-after.txt").write_text(after_metrics)
        result = {"implementation": label, "image": image, "image_id": inspection["Image"], "processes": command("docker", "top", cid, "-eo", "pid,ppid,comm,args"), "limits": {"cpus": cpus, "memory_bytes": 1073741824, "worker_threads": workers}, "load": load, "cgroup_before": before, "cgroup_after": after}
        if idle_observation:
            result["idle_observation"] = idle_observation
        result["observation_seconds"] = observation_seconds
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
    parser.add_argument("--baseline-label", default="elixir")
    parser.add_argument("--candidate-label", default="rust")
    parser.add_argument("--pairs", type=int, default=64)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--protocol", choices=["v1", "v2"], default="v2")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--idle", action="store_true", help="measure established idle sockets, then verify bidirectional delivery")
    modes.add_argument("--paced", action="store_true", help="send 200 frames/s per pair to compare CPU at equal traffic")
    parser.add_argument("--cpus", type=int, choices=[1, 2], default=2)
    parser.add_argument("--workers", type=int, choices=[1, 2], default=2)
    args = parser.parse_args()
    if not (1 <= args.pairs <= 20000 and 1 <= args.seconds <= 3600 and 16 <= args.payload_bytes <= 33554418 and 1 <= args.repetitions <= 20):
        parser.error("invalid workload bounds")
    args.output.mkdir(parents=True, exist_ok=False)
    results = []
    metadata = {"git_commit": command("git", "rev-parse", "HEAD"), "dirty": bool(command("git", "status", "--porcelain")), "host": platform.platform(), "cpu": next(line.split(":",1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "workload": {"pairs": args.pairs, "seconds": args.seconds, "payload_bytes": args.payload_bytes, "protocol": args.protocol, "idle": args.idle, "paced": args.paced, "cpus": args.cpus, "workers": args.workers}}
    sources = [Path("rust/Cargo.toml"), Path("rust/Cargo.lock"), Path("Dockerfile"), Path("Dockerfile.elixir"), Path("scripts/ci-rust.sh"), Path("rust/examples/relay_bench.rs"), Path(__file__)] + sorted(Path("rust/src").glob("*.rs"))
    metadata["source_dirty"] = bool(command("git", "status", "--porcelain", "--", *(str(path) for path in sources)))
    metadata["source_sha256"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    metadata["bench_binary_sha256"] = hashlib.sha256(args.bench.read_bytes()).hexdigest()
    (args.output / "metadata.json").write_text(json.dumps(metadata,indent=2)+"\n")
    for repetition in range(args.repetitions):
        order = [(args.baseline_label, args.elixir_image), (args.candidate_label, args.rust_image)]
        if repetition % 2:
            order.reverse()
        for label, image in order:
            results.append(run(args, image, label, args.output / f"{repetition+1}-{label}"))
            (args.output / "results.json").write_text(json.dumps(results,indent=2)+"\n")


if __name__ == "__main__":
    main()
