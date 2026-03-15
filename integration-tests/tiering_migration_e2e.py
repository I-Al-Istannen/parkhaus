#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "boto3>=1.37.0",
#   "botocore>=1.37.0",
#   "rich>=13.9.0",
#   "testcontainers>=4.13.0",
# ]
# ///

"""End-to-end tiering migration test.

This script starts three S3 backends (hot/warm/cold) via Python testcontainers,
starts the Rust proxy, uploads a few hundred objects through the proxy, verifies
all are initially on hot, then runs two randomized timestamp cycles across all
objects to force migrations in all directions (including upward cold->warm/hot
and warm->hot), validating DB and physical placement after each cycle.

Run:
    uv run --script integration-tests/tiering_migration_e2e.py
"""

from __future__ import annotations

import argparse
import random
import socket
import sqlite3
import subprocess
import tempfile
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import boto3
from botocore.exceptions import ClientError
from rich.console import Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.text import Text
from testcontainers.core.container import DockerContainer


ACCESS_KEY = "test-access-key"
SECRET_KEY = "test-secret-key"
REGION = "us-east-1"
CONTAINER_S3_PORT = 9090


@dataclass
class Backend:
    name: str
    container: DockerContainer
    endpoint: str


class TestFailure(RuntimeError):
    pass


class Ui:
    def __init__(self, seed: int, object_count: int, bucket: str) -> None:
        self.seed = seed
        self.object_count = object_count
        self.bucket = bucket
        self.current_stage = "initializing"
        self.status = "starting"
        self.logs: deque[str] = deque(maxlen=30)
        self.completed_stages: deque[str] = deque(maxlen=20)
        self.proxy_log_path: Path | None = None

        self.progress = Progress(
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        )
        self.stage_task: TaskID = self.progress.add_task(
            "phases", total=12, completed=0
        )
        self.work_task: TaskID = self.progress.add_task("current", total=1, completed=0)

        self.layout = Layout()
        self.layout.split_column(
            Layout(name="top", ratio=2),
            Layout(name="logs", ratio=1),
        )
        self.live: Live | None = None

    def __enter__(self) -> "Ui":
        self.live = Live(self.layout, refresh_per_second=4)
        self.live.__enter__()
        self.refresh()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.live is not None:
            self.live.__exit__(exc_type, exc, tb)

    def set_proxy_log(self, path: Path) -> None:
        self.proxy_log_path = path

    def stage(self, description: str) -> None:
        if self.current_stage != "initializing":
            self.completed_stages.append(self.current_stage)
        self.current_stage = description
        self.status = description
        self.progress.advance(self.stage_task, 1)
        self.refresh()

    def set_work(self, description: str, total: float, completed: float = 0) -> None:
        self.progress.update(
            self.work_task, description=description, total=total, completed=completed
        )
        self.refresh()

    def advance_work(self, amount: float = 1) -> None:
        self.progress.advance(self.work_task, amount)
        self.refresh()

    def complete_work(self, description: str | None = None) -> None:
        if description is not None:
            self.progress.update(self.work_task, description=description)
            self.status = description
        self.progress.update(
            self.work_task, completed=self.progress.tasks[self.work_task].total
        )
        self.refresh()

    def note(self, text: str) -> None:
        self.status = text
        self.logs.append(text)
        self.refresh()

    def tick(self) -> None:
        self.refresh()

    def refresh(self) -> None:
        if self.proxy_log_path is not None and self.proxy_log_path.exists():
            lines = self.proxy_log_path.read_text(
                encoding="utf-8", errors="replace"
            ).splitlines()
            tail = lines[-20:]
            if not tail:
                tail = ["<proxy log empty>"]
        else:
            tail = ["<proxy log unavailable>"]

        completed_lines = (
            "\n".join(f"- {stage}" for stage in self.completed_stages)
            if self.completed_stages
            else "- <none yet>"
        )

        summary = Text(
            f"seed={self.seed}  objects={self.object_count}  bucket={self.bucket}\n"
            f"stage={self.current_stage}\n"
            f"status={self.status}\n\n"
            f"completed stages:\n{completed_lines}"
        )
        self.layout["top"].update(
            Panel(
                Group(self.progress, summary),
                title="Tiering E2E Progress",
                border_style="cyan",
            )
        )
        self.layout["logs"].update(
            Panel(
                "\n".join(tail), title="Server Logs (proxy tail)", border_style="green"
            )
        )
        if self.live is not None:
            self.live.refresh()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tiering proxy E2E migration test")
    parser.add_argument("--object-count", type=int, default=600)
    parser.add_argument("--bucket", default="tiering-e2e-bucket")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--s3mock-image", default="adobe/s3mock:latest")
    parser.add_argument("--proxy-start-timeout-seconds", type=int, default=90)
    parser.add_argument("--backend-start-timeout-seconds", type=int, default=120)
    parser.add_argument("--migration-timeout-seconds", type=int, default=420)
    parser.add_argument("--poll-interval-seconds", type=float, default=5.0)
    parser.add_argument(
        "--object-prefix",
        default=None,
        help="Defaults to tiering-e2e-<seed>",
    )
    return parser.parse_args()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_tcp(host: str, port: int, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise TestFailure(f"Timed out waiting for TCP {host}:{port}")


def s3_client(endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )


def object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.create_bucket(Bucket=bucket)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            return
        raise


def start_backend(name: str, image: str, startup_timeout_seconds: int) -> Backend:
    container = DockerContainer(image).with_exposed_ports(CONTAINER_S3_PORT)
    container.start()
    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(CONTAINER_S3_PORT))
    endpoint = f"http://{host}:{port}"

    client = s3_client(endpoint)
    deadline = time.monotonic() + startup_timeout_seconds
    while True:
        try:
            client.list_buckets()
            break
        except Exception:
            if time.monotonic() >= deadline:
                container.stop()
                raise TestFailure(f"Timed out waiting for backend {name} at {endpoint}")
            time.sleep(0.5)

    return Backend(name=name, container=container, endpoint=endpoint)


def write_proxy_config(
    path: Path, listen_port: int, db_path: Path, backends: list[Backend]
) -> None:
    endpoint_by_name = {backend.name: backend.endpoint for backend in backends}
    text = f"""listen = \"127.0.0.1:{listen_port}\"
db_path = \"{db_path.as_posix()}\"

[upstreams.hot]
order = 1
base_url = \"{endpoint_by_name["hot"]}\"
addressing_style = \"path\"
max_age = \"5m\"
s3_access_key = \"{ACCESS_KEY}\"
s3_secret = \"{SECRET_KEY}\"
region = \"{REGION}\"

[upstreams.warm]
order = 2
base_url = \"{endpoint_by_name["warm"]}\"
addressing_style = \"path\"
max_age = \"20m\"
s3_access_key = \"{ACCESS_KEY}\"
s3_secret = \"{SECRET_KEY}\"
region = \"{REGION}\"

[upstreams.cold]
order = 3
base_url = \"{endpoint_by_name["cold"]}\"
addressing_style = \"path\"
s3_access_key = \"{ACCESS_KEY}\"
s3_secret = \"{SECRET_KEY}\"
region = \"{REGION}\"
"""
    path.write_text(text, encoding="utf-8")


def start_proxy(repo_root: Path, config_path: Path, log_path: Path) -> subprocess.Popen:
    log_file = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        ["cargo", "run", "--", "--config", str(config_path), "serve"],
        cwd=repo_root,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    process._log_file = log_file  # type: ignore[attr-defined]
    return process


def stop_proxy(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)

    log_file = getattr(process, "_log_file", None)
    if log_file is not None:
        log_file.close()


def read_proxy_logs_tail(log_path: Path, lines: int = 40) -> str:
    if not log_path.exists():
        return "<no proxy log file>"

    all_lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    tail = all_lines[-lines:]
    return "\n".join(tail) if tail else "<proxy log empty>"


def upload_objects(
    client,
    bucket: str,
    keys: list[str],
    on_item: Callable[[], None] | None = None,
) -> None:
    for key in keys:
        payload = f"payload-for-{key}".encode("utf-8")
        client.put_object(Bucket=bucket, Key=key, Body=payload)
        if on_item is not None:
            on_item()


def wait_for_db_record_count(
    db_path: Path,
    bucket: str,
    prefix: str,
    expected: int,
    timeout_seconds: int,
    on_poll: Callable[[int], None] | None = None,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    query = "SELECT COUNT(*) FROM Objects WHERE bucket = ? AND key LIKE ?"

    while time.monotonic() < deadline:
        with sqlite3.connect(db_path) as conn:
            count = conn.execute(query, (bucket, f"{prefix}%")).fetchone()[0]
        if on_poll is not None:
            on_poll(count)
        if count == expected:
            return
        time.sleep(0.5)

    raise TestFailure(f"Timed out waiting for DB records, expected={expected}.")


def db_assignment_counts(db_path: Path, bucket: str, prefix: str) -> dict[str, int]:
    query = (
        "SELECT assigned_upstream, COUNT(*) FROM Objects "
        "WHERE bucket = ? AND key LIKE ? GROUP BY assigned_upstream"
    )
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, (bucket, f"{prefix}%")).fetchall()
    return {name: count for name, count in rows}


def pending_count(db_path: Path, bucket: str, prefix: str) -> int:
    query = "SELECT COUNT(*) FROM PendingMigrations WHERE bucket = ? AND key LIKE ?"
    with sqlite3.connect(db_path) as conn:
        return int(conn.execute(query, (bucket, f"{prefix}%")).fetchone()[0])


def assert_all_hot(
    db_path: Path, bucket: str, prefix: str, expected_count: int
) -> None:
    counts = db_assignment_counts(db_path, bucket, prefix)
    hot_count = counts.get("hot", 0)
    total = sum(counts.values())
    if total != expected_count or hot_count != expected_count:
        raise TestFailure(
            f"Expected all objects on hot. counts={counts}, expected={expected_count}"
        )


def db_assignments_by_key(db_path: Path, bucket: str, prefix: str) -> dict[str, str]:
    query = "SELECT key, assigned_upstream FROM Objects WHERE bucket = ? AND key LIKE ?"
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, (bucket, f"{prefix}%")).fetchall()
    return {key: upstream for key, upstream in rows}


def set_object_timestamps(
    db_path: Path,
    bucket: str,
    timestamp_by_key: dict[str, int],
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            "UPDATE Objects SET last_modified = ? WHERE bucket = ? AND key = ?",
            [
                (timestamp_ms, bucket, key)
                for key, timestamp_ms in timestamp_by_key.items()
            ],
        )
        conn.commit()


def random_targets_for_all(
    keys: list[str],
    rng: random.Random,
    min_per_tier: int,
) -> dict[str, str]:
    if len(keys) < min_per_tier * 3:
        raise TestFailure(
            f"need at least {min_per_tier * 3} keys to ensure all target tiers are represented"
        )

    shuffled = keys.copy()
    rng.shuffle(shuffled)
    targets: dict[str, str] = {}

    for index, tier in enumerate(("hot", "warm", "cold")):
        start = index * min_per_tier
        end = start + min_per_tier
        for key in shuffled[start:end]:
            targets[key] = tier

    for key in shuffled[min_per_tier * 3 :]:
        targets[key] = rng.choices(
            ["hot", "warm", "cold"], weights=[0.34, 0.33, 0.33], k=1
        )[0]

    return targets


def expected_counts(targets: dict[str, str]) -> dict[str, int]:
    counts = {"hot": 0, "warm": 0, "cold": 0}
    for tier in targets.values():
        counts[tier] += 1
    return counts


def build_random_timestamps_for_targets(
    targets: dict[str, str],
    now_ms: int,
    rng: random.Random,
) -> dict[str, int]:
    timestamp_by_key: dict[str, int] = {}
    for key, target in targets.items():
        if target == "hot":
            age_ms = rng.randint(20_000, 120_000)
        elif target == "warm":
            age_ms = rng.randint(420_000, 720_000)
        elif target == "cold":
            age_ms = rng.randint(1_800_000, 2_700_000)
        else:
            raise TestFailure(f"unknown target tier: {target}")
        timestamp_by_key[key] = now_ms - age_ms

    return timestamp_by_key


def enforce_upward_targets(
    targets: dict[str, str],
    current_by_key: dict[str, str],
    rng: random.Random,
) -> dict[str, int]:
    cold_keys = [key for key, tier in current_by_key.items() if tier == "cold"]
    warm_keys = [key for key, tier in current_by_key.items() if tier == "warm"]

    if not cold_keys or not warm_keys:
        raise TestFailure(
            "need existing cold and warm assignments before reverse cycle; "
            f"have cold={len(cold_keys)} warm={len(warm_keys)}"
        )

    cold_to_hot = max(2, len(cold_keys) // 15)
    cold_to_warm = max(2, len(cold_keys) // 15)
    warm_to_hot = max(2, len(warm_keys) // 15)

    cold_to_hot = min(cold_to_hot, len(cold_keys))
    remaining_for_warm = max(0, len(cold_keys) - cold_to_hot)
    cold_to_warm = min(cold_to_warm, remaining_for_warm)
    warm_to_hot = min(warm_to_hot, len(warm_keys))

    chosen_cold = cold_keys.copy()
    rng.shuffle(chosen_cold)
    chosen_warm = warm_keys.copy()
    rng.shuffle(chosen_warm)

    cold_to_hot_keys = chosen_cold[:cold_to_hot]
    cold_to_warm_keys = chosen_cold[cold_to_hot : cold_to_hot + cold_to_warm]
    warm_to_hot_keys = chosen_warm[:warm_to_hot]

    for key in cold_to_hot_keys:
        targets[key] = "hot"
    for key in cold_to_warm_keys:
        targets[key] = "warm"
    for key in warm_to_hot_keys:
        targets[key] = "hot"

    return {
        "cold_to_hot": len(cold_to_hot_keys),
        "cold_to_warm": len(cold_to_warm_keys),
        "warm_to_hot": len(warm_to_hot_keys),
    }


def wait_for_expected_distribution(
    db_path: Path,
    bucket: str,
    prefix: str,
    expected: dict[str, int],
    timeout_seconds: int,
    poll_interval_seconds: float,
    on_poll: Callable[[dict[str, int], int], None] | None = None,
) -> tuple[dict[str, int], int]:
    deadline = time.monotonic() + timeout_seconds
    last_counts: dict[str, int] = {}
    last_pending = 0

    while time.monotonic() < deadline:
        counts = db_assignment_counts(db_path, bucket, prefix)
        pending = pending_count(db_path, bucket, prefix)
        last_counts = counts
        last_pending = pending

        if on_poll is not None:
            on_poll(counts, pending)

        if pending == 0 and all(
            counts.get(tier, 0) == count for tier, count in expected.items()
        ):
            return counts, pending

        time.sleep(poll_interval_seconds)

    return last_counts, last_pending


def assert_backend_placement(
    clients: dict[str, object],
    bucket: str,
    expected_by_key: dict[str, str],
    on_item: Callable[[int], None] | None = None,
) -> None:
    backend_names = sorted(clients.keys())

    for idx, (key, expected_backend) in enumerate(expected_by_key.items(), start=1):
        for backend_name in backend_names:
            exists = object_exists(clients[backend_name], bucket, key)
            should_exist = backend_name == expected_backend
            if exists != should_exist:
                raise TestFailure(
                    "Backend placement mismatch "
                    f"for key={key} backend={backend_name} "
                    f"expected={should_exist} actual={exists}"
                )

        if on_item is not None:
            on_item(idx)


def assert_proxy_and_direct_reads_match(
    proxy_client,
    backend_clients: dict[str, object],
    bucket: str,
    expected_by_key: dict[str, str],
    on_item: Callable[[int], None] | None = None,
) -> None:
    for idx, (key, expected_backend) in enumerate(expected_by_key.items(), start=1):
        try:
            proxy_response = proxy_client.get_object(Bucket=bucket, Key=key)
        except ClientError as error:
            raise TestFailure(f"proxy read failed for key={key}: {error}") from error

        try:
            direct_response = backend_clients[expected_backend].get_object(
                Bucket=bucket, Key=key
            )
        except ClientError as error:
            raise TestFailure(
                f"direct read failed for key={key} backend={expected_backend}: {error}"
            ) from error

        proxy_body = proxy_response["Body"].read()
        direct_body = direct_response["Body"].read()

        if proxy_body != direct_body:
            raise TestFailure(
                "Proxy/direct body mismatch "
                f"for key={key} backend={expected_backend} "
                f"proxy_len={len(proxy_body)} direct_len={len(direct_body)}"
            )

        if on_item is not None:
            on_item(idx)


def main() -> int:
    args = parse_args()
    if args.object_count < 300:
        raise TestFailure("Use at least a few hundred objects (>= 300)")

    repo_root = Path(__file__).resolve().parents[1]

    seed = args.seed if args.seed is not None else int(time.time())
    rng = random.Random(seed)

    prefix = args.object_prefix or f"tiering-e2e-{seed}-"
    object_keys = [f"{prefix}{i:06d}" for i in range(args.object_count)]

    listen_port = free_port()
    proxy_process: subprocess.Popen | None = None
    backends: list[Backend] = []

    start_time = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="tiering-e2e-") as temp_dir:
        temp = Path(temp_dir)
        config_path = temp / "config.toml"
        db_path = temp / "tiering.sqlite"
        proxy_log_path = temp / "proxy.log"

        try:
            with Ui(
                seed=seed, object_count=args.object_count, bucket=args.bucket
            ) as ui:
                ui.set_proxy_log(proxy_log_path)
                ui.note(f"listen port={listen_port}")

                ui.stage("start backends")
                ui.set_work("starting hot/warm/cold backends", total=3, completed=0)
                for name in ("hot", "warm", "cold"):
                    backend = start_backend(
                        name=name,
                        image=args.s3mock_image,
                        startup_timeout_seconds=args.backend_start_timeout_seconds,
                    )
                    backends.append(backend)
                    ui.note(f"started {name} at {backend.endpoint}")
                    ui.advance_work()

                ui.stage("write config and start proxy")
                ui.set_work("waiting for proxy tcp readiness", total=1, completed=0)
                write_proxy_config(config_path, listen_port, db_path, backends)
                proxy_process = start_proxy(repo_root, config_path, proxy_log_path)
                wait_for_tcp("127.0.0.1", listen_port, args.proxy_start_timeout_seconds)
                ui.complete_work("proxy reachable")

                proxy_endpoint = f"http://127.0.0.1:{listen_port}"
                proxy_client = s3_client(proxy_endpoint)
                backend_clients = {b.name: s3_client(b.endpoint) for b in backends}

                ui.stage("prepare buckets")
                ui.set_work("ensuring buckets on all backends", total=4, completed=0)
                for client in backend_clients.values():
                    ensure_bucket(client, args.bucket)
                    ui.advance_work()

                # Also create via proxy to exercise bucket-level routing.
                ensure_bucket(proxy_client, args.bucket)
                ui.advance_work()

                ui.stage("upload objects")
                ui.set_work(
                    "uploading objects via proxy", total=len(object_keys), completed=0
                )
                upload_objects(
                    proxy_client, args.bucket, object_keys, on_item=ui.advance_work
                )

                ui.stage("verify initial hot placement")
                ui.set_work(
                    "waiting for DB object records",
                    total=args.object_count,
                    completed=0,
                )
                wait_for_db_record_count(
                    db_path=db_path,
                    bucket=args.bucket,
                    prefix=prefix,
                    expected=args.object_count,
                    timeout_seconds=30,
                    on_poll=lambda count: ui.set_work(
                        "waiting for DB object records",
                        total=args.object_count,
                        completed=min(count, args.object_count),
                    ),
                )
                assert_all_hot(db_path, args.bucket, prefix, args.object_count)
                ui.note("verified all objects assigned to hot in DB")

                initial_expected = {key: "hot" for key in object_keys}
                ui.set_work(
                    "verifying hot placement and read parity",
                    total=len(object_keys) * 2,
                    completed=0,
                )
                assert_backend_placement(
                    backend_clients,
                    args.bucket,
                    initial_expected,
                    on_item=lambda idx: ui.set_work(
                        "verifying hot placement and read parity",
                        total=len(object_keys) * 2,
                        completed=idx,
                    ),
                )
                ui.note("verified physical placement on hot")
                assert_proxy_and_direct_reads_match(
                    proxy_client,
                    backend_clients,
                    args.bucket,
                    initial_expected,
                    on_item=lambda idx: ui.set_work(
                        "verifying hot placement and read parity",
                        total=len(object_keys) * 2,
                        completed=len(object_keys) + idx,
                    ),
                )
                ui.note("verified proxy/direct read parity after initial placement")

                ui.stage("apply random cycle-1 timestamps")
                cycle1_targets = random_targets_for_all(
                    object_keys,
                    rng,
                    min_per_tier=max(10, len(object_keys) // 10),
                )
                cycle1_expected = expected_counts(cycle1_targets)
                ui.set_work(
                    "updating cycle-1 timestamps for all objects", total=1, completed=0
                )
                set_object_timestamps(
                    db_path,
                    args.bucket,
                    build_random_timestamps_for_targets(
                        cycle1_targets, int(time.time() * 1000), rng
                    ),
                )
                ui.complete_work(f"cycle1 targets={cycle1_expected}")

                ui.stage("wait for cycle-1 migration")
                ui.set_work(
                    "polling DB for cycle-1 convergence",
                    total=args.migration_timeout_seconds,
                    completed=0,
                )

                cycle1_poll_state = {"elapsed": 0.0}

                def on_cycle1_poll(counts: dict[str, int], pending: int) -> None:
                    cycle1_poll_state["elapsed"] = min(
                        args.migration_timeout_seconds,
                        cycle1_poll_state["elapsed"] + args.poll_interval_seconds,
                    )
                    ui.set_work(
                        "polling DB for cycle-1 convergence",
                        total=args.migration_timeout_seconds,
                        completed=cycle1_poll_state["elapsed"],
                    )
                    ui.note(f"cycle1 counts={counts} pending={pending}")
                    ui.tick()

                cycle1_counts, cycle1_pending = wait_for_expected_distribution(
                    db_path=db_path,
                    bucket=args.bucket,
                    prefix=prefix,
                    expected=cycle1_expected,
                    timeout_seconds=args.migration_timeout_seconds,
                    poll_interval_seconds=args.poll_interval_seconds,
                    on_poll=on_cycle1_poll,
                )

                if not (
                    cycle1_pending == 0
                    and cycle1_counts.get("hot", 0) == cycle1_expected["hot"]
                    and cycle1_counts.get("warm", 0) == cycle1_expected["warm"]
                    and cycle1_counts.get("cold", 0) == cycle1_expected["cold"]
                ):
                    raise TestFailure(
                        "Cycle-1 migration did not reach expected outcome within timeout. "
                        f"counts={cycle1_counts}, expected={cycle1_expected}, pending={cycle1_pending}\n"
                        f"Proxy log tail:\n{read_proxy_logs_tail(proxy_log_path)}"
                    )

                ui.stage("verify cycle-1 placement")
                ui.set_work(
                    "verifying cycle-1 placement and read parity",
                    total=len(object_keys) * 2,
                    completed=0,
                )
                assert_backend_placement(
                    backend_clients,
                    args.bucket,
                    cycle1_targets,
                    on_item=lambda idx: ui.set_work(
                        "verifying cycle-1 placement and read parity",
                        total=len(object_keys) * 2,
                        completed=idx,
                    ),
                )
                assert_proxy_and_direct_reads_match(
                    proxy_client,
                    backend_clients,
                    args.bucket,
                    cycle1_targets,
                    on_item=lambda idx: ui.set_work(
                        "verifying cycle-1 placement and read parity",
                        total=len(object_keys) * 2,
                        completed=len(object_keys) + idx,
                    ),
                )
                ui.note("verified proxy/direct read parity after cycle-1")

                ui.stage("apply random cycle-2 timestamps")
                cycle2_targets = random_targets_for_all(
                    object_keys,
                    rng,
                    min_per_tier=max(10, len(object_keys) // 10),
                )
                current_assignments = db_assignments_by_key(
                    db_path, args.bucket, prefix
                )
                upward = enforce_upward_targets(
                    cycle2_targets, current_assignments, rng
                )
                cycle2_expected = expected_counts(cycle2_targets)

                ui.set_work(
                    "updating cycle-2 timestamps for all objects", total=1, completed=0
                )
                set_object_timestamps(
                    db_path,
                    args.bucket,
                    build_random_timestamps_for_targets(
                        cycle2_targets, int(time.time() * 1000), rng
                    ),
                )
                ui.advance_work()
                ui.note(
                    "cycle2 upward targets: "
                    f"cold->hot={upward['cold_to_hot']} "
                    f"cold->warm={upward['cold_to_warm']} "
                    f"warm->hot={upward['warm_to_hot']} "
                    f"expected={cycle2_expected}"
                )

                ui.stage("wait for cycle-2 migration")
                ui.set_work(
                    "polling DB for cycle-2 convergence",
                    total=args.migration_timeout_seconds,
                    completed=0,
                )

                cycle2_poll_state = {"elapsed": 0.0}

                def on_cycle2_poll(counts: dict[str, int], pending: int) -> None:
                    cycle2_poll_state["elapsed"] = min(
                        args.migration_timeout_seconds,
                        cycle2_poll_state["elapsed"] + args.poll_interval_seconds,
                    )
                    ui.set_work(
                        "polling DB for cycle-2 convergence",
                        total=args.migration_timeout_seconds,
                        completed=cycle2_poll_state["elapsed"],
                    )
                    ui.note(f"cycle2 counts={counts} pending={pending}")
                    ui.tick()

                cycle2_counts, cycle2_pending = wait_for_expected_distribution(
                    db_path=db_path,
                    bucket=args.bucket,
                    prefix=prefix,
                    expected=cycle2_expected,
                    timeout_seconds=args.migration_timeout_seconds,
                    poll_interval_seconds=args.poll_interval_seconds,
                    on_poll=on_cycle2_poll,
                )

                if not (
                    cycle2_pending == 0
                    and cycle2_counts.get("hot", 0) == cycle2_expected["hot"]
                    and cycle2_counts.get("warm", 0) == cycle2_expected["warm"]
                    and cycle2_counts.get("cold", 0) == cycle2_expected["cold"]
                ):
                    raise TestFailure(
                        "Cycle-2 migration did not reach expected outcome within timeout. "
                        f"counts={cycle2_counts}, expected={cycle2_expected}, pending={cycle2_pending}\n"
                        f"Proxy log tail:\n{read_proxy_logs_tail(proxy_log_path)}"
                    )

                ui.stage("verify cycle-2 placement")
                ui.set_work(
                    "verifying cycle-2 placement and read parity",
                    total=len(object_keys) * 2,
                    completed=0,
                )
                assert_backend_placement(
                    backend_clients,
                    args.bucket,
                    cycle2_targets,
                    on_item=lambda idx: ui.set_work(
                        "verifying cycle-2 placement and read parity",
                        total=len(object_keys) * 2,
                        completed=idx,
                    ),
                )
                assert_proxy_and_direct_reads_match(
                    proxy_client,
                    backend_clients,
                    args.bucket,
                    cycle2_targets,
                    on_item=lambda idx: ui.set_work(
                        "verifying cycle-2 placement and read parity",
                        total=len(object_keys) * 2,
                        completed=len(object_keys) + idx,
                    ),
                )
                ui.note("verified proxy/direct read parity after cycle-2")

                ui.stage("done")
                elapsed = time.monotonic() - start_time
                ui.set_work("finished", total=1, completed=1)
                ui.note(
                    "success "
                    f"cycle1_counts={cycle1_counts} "
                    f"cycle2_counts={cycle2_counts} "
                    f"upward={upward} "
                    f"elapsed={elapsed:.1f}s"
                )

                time.sleep(0.3)

            elapsed = time.monotonic() - start_time
            print("Migration verification successful")
            print(f"Cycle 1 counts: {cycle1_counts}")
            print(f"Cycle 2 counts: {cycle2_counts}")
            print(f"Upward transitions guaranteed in cycle 2: {upward}")
            print(f"Elapsed seconds: {elapsed:.1f}")
            print(f"Proxy log: {proxy_log_path}")
            return 0
        finally:
            if proxy_process is not None:
                stop_proxy(proxy_process)
            for backend in reversed(backends):
                try:
                    backend.container.stop()
                except Exception as error:
                    print(f"Warning: failed to stop backend {backend.name}: {error}")


def entrypoint() -> int:
    try:
        return main()
    except KeyboardInterrupt:
        print("Interrupted")
        return 130
    except TestFailure as error:
        print(f"TEST FAILURE: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(entrypoint())
