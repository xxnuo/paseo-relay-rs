#!/usr/bin/env -S uv run python
"""Run actual Paseo client/daemon source against an isolated relay Docker image."""

import argparse
import hashlib
import importlib.util
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "compare", ROOT / "scripts/compare-rust.py"
)
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--paseo-source", type=Path, default=ROOT.parent / "paseo")
    parser.add_argument("--image", default="paseo-relay-rust:real-clients")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument(
        "--netem",
        action="store_true",
        help="exercise 100ms egress delay/jitter and 0.5% loss in the relay network namespace",
    )
    parser.add_argument("--clients", type=int, default=32)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args()
    if not (
        2 <= args.clients <= 512
        and 1 <= args.seconds <= 300
        and 30 <= args.timeout <= 1800
    ):
        parser.error("invalid workload bounds")
    source = args.paseo_source.resolve()
    if not (source / "node_modules/tsx").exists():
        parser.error(
            "Paseo checkout must have its own installed workspace dependencies"
        )
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    if not args.no_build:
        with (output / "docker-build.log").open("w") as log:
            subprocess.run(
                ["docker", "build", "-t", args.image, "."],
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )
    if args.netem:
        with (output / "netem-build.log").open("w") as log:
            subprocess.run(
                [
                    "docker",
                    "build",
                    "-f",
                    "tests/real-clients/netem.Dockerfile",
                    "-t",
                    "paseo-relay-netem:test",
                    ".",
                ],
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )
    cid = None
    child = None
    passed = False
    samples = []
    started = time.monotonic()
    metadata = {
        "relay_commit": compare.command("git", "-C", str(ROOT), "rev-parse", "HEAD"),
        "relay_source_dirty": bool(
            compare.command(
                "git",
                "-C",
                str(ROOT),
                "status",
                "--porcelain",
                "--",
                "rust",
                "Dockerfile",
            )
        ),
        "paseo_commit": compare.command("git", "-C", str(source), "rev-parse", "HEAD"),
        "paseo_initial_status": compare.command(
            "git", "-C", str(source), "status", "--porcelain"
        ),
        "node_version": compare.command("node", "--version"),
        "paseo_package_lock_sha256": hashlib.sha256(
            (source / "package-lock.json").read_bytes()
        ).hexdigest(),
        "clients": args.clients,
        "load_seconds": args.seconds,
        "netem": args.netem,
        "limits": {"cpus": 1, "memory_bytes": 536870912, "runtime_workers": 1},
        "scope": "real local client/daemon sources and relay Docker sockets; no provider chat, UI, TLS, Cloudflare or WAN test",
        "harness_sha256": {
            p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest()
            for p in [
                "scripts/test-real-clients.py",
                "tests/real-clients/scenarios.mjs",
                "tests/real-clients/netem.Dockerfile",
            ]
        },
    }
    try:
        cid = "paseo-real-clients-" + uuid.uuid4().hex
        # Docker reallocates an automatic HostPort on restart; a VPS endpoint is stable.
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            stable_port = reservation.getsockname()[1]
        cid = compare.command(
            "docker",
            "run",
            "--detach",
            "--name",
            cid,
            "--cpus=1",
            "--memory=512m",
            "--pids-limit=256",
            "--ulimit=nofile=65536:65536",
            "--publish",
            f"127.0.0.1:{stable_port}:4000",
            "--env",
            "TOKIO_WORKER_THREADS=1",
            "--env",
            "ERL_FLAGS=+S 1:1 +A 2",
            "--env",
            "PASEO_RELAY_HOST=0.0.0.0",
            "--env",
            "PASEO_RELAY_INGRESS_BUDGET_BYTES=134217728",
            "--env",
            "PASEO_RELAY_MEMORY_WATERMARK_BYTES=402653184",
            args.image,
        )
        inspection = json.loads(compare.command("docker", "inspect", cid))[0]
        metadata["image_id"] = inspection["Image"]
        port = inspection["NetworkSettings"]["Ports"]["4000/tcp"][0]["HostPort"]
        metadata["stable_host_port"] = int(port)
        base = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 20
        while True:
            try:
                assert json.loads(compare.get(base + "/ready")) == {"status": "ready"}
                break
            except (OSError, AssertionError):
                if time.monotonic() >= deadline:
                    raise RuntimeError("relay not ready")
                time.sleep(0.05)
        (output / "metrics-before.txt").write_text(compare.get(base + "/metrics"))
        with (
            tempfile.TemporaryDirectory(prefix="paseo-real-clients-") as work,
            (output / "client.stdout").open("w") as stdout,
            (output / "client.stderr").open("w") as stderr,
        ):
            env = {
                **os.environ,
                "PASEO_SOURCE": str(source),
                "PASEO_TEST_WORK": work,
                "PASEO_TEST_OUTPUT": str(output),
                "PASEO_RELAY_ENDPOINT": f"127.0.0.1:{port}",
                "PASEO_RELAY_CONTAINER": cid,
                "PASEO_STRESS_CLIENTS": str(args.clients),
                "PASEO_STRESS_SECONDS": str(args.seconds),
                "PASEO_NETEM": "1" if args.netem else "0",
            }
            child = subprocess.Popen(
                ["node", str(ROOT / "tests/real-clients/scenarios.mjs")],
                env=env,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
            deadline = time.monotonic() + args.timeout
            try:
                while child.poll() is None:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("real-client suite exceeded deadline")
                    try:
                        info = json.loads(compare.command("docker", "inspect", cid))[0]
                        if info["State"]["Running"]:
                            metrics = compare.get(base + "/metrics")
                            samples.append(
                                {
                                    "elapsed_seconds": time.monotonic() - started,
                                    "metrics": metrics,
                                    "cgroup": compare.cgroup(info["State"]["Pid"]),
                                }
                            )
                    except OSError:
                        pass  # A planned container restart temporarily removes the listener/cgroup.
                    time.sleep(0.5)
            finally:
                if child.poll() is None:
                    os.killpg(child.pid, signal.SIGTERM)
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.wait()
            if child.returncode:
                raise RuntimeError(
                    f"real-client suite failed ({child.returncode}); see {output}/client.stderr and client.stdout"
                )
        deadline = time.monotonic() + 10
        while True:
            metrics = compare.get(base + "/metrics")
            gauges = compare.gauges(metrics)
            if (
                int(gauges["paseo_relay_active_websockets"])
                == int(gauges["paseo_relay_ingress_reserved_bytes"])
                == 0
            ):
                break
            if time.monotonic() >= deadline:
                raise AssertionError("client cleanup leaked sockets or ingress bytes")
            time.sleep(0.05)
        (output / "metrics-after.txt").write_text(metrics)
        assert samples, "no runtime samples"
        for sample in samples:
            events = dict(
                line.split() for line in sample["cgroup"]["memory.events"].splitlines()
            )
            assert int(events["oom"]) == int(events["oom_kill"]) == 0
        assert json.loads((output / "client-results.json").read_text())["passed"]
        final_info = json.loads(compare.command("docker", "inspect", cid))[0]
        assert final_info["NetworkSettings"]["Ports"]["4000/tcp"][0]["HostPort"] == port
        assert not final_info["State"]["OOMKilled"]
        events = subprocess.run(
            [
                "docker",
                "events",
                "--since",
                str(int(time.time() - (time.monotonic() - started)) - 1),
                "--until",
                str(int(time.time()) + 1),
                "--filter",
                "container=" + cid,
                "--filter",
                "event=oom",
                "--format",
                "{{json .}}",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=8,
        )
        (output / "oom-events.jsonl").write_text(events.stdout)
        assert not events.stdout.strip(), (
            "Docker recorded OOM before/after the planned restart"
        )
        assert (
            compare.command("git", "-C", str(source), "rev-parse", "HEAD")
            == metadata["paseo_commit"]
        )
        assert (
            compare.command("git", "-C", str(source), "status", "--porcelain")
            == metadata["paseo_initial_status"]
        ), "reference checkout changed during the test"
        passed = True
    finally:
        metadata.update(
            {
                "passed": passed,
                "elapsed_seconds": time.monotonic() - started,
                "paseo_final_status": compare.command(
                    "git", "-C", str(source), "status", "--porcelain"
                ),
            }
        )
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        (output / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
        if cid:
            logs = subprocess.run(
                ["docker", "logs", cid], check=False, capture_output=True, text=True
            )
            (output / "relay.log").write_text(logs.stdout + logs.stderr)
            subprocess.run(
                ["docker", "rm", "--force", cid], check=True, stdout=subprocess.DEVNULL
            )
    print(
        json.dumps(
            {
                "passed": passed,
                "output": str(output),
                "seconds": metadata["elapsed_seconds"],
            }
        )
    )


if __name__ == "__main__":
    main()
