import contextlib
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

import boto3
import requests
from mypy_boto3_s3 import S3Client
from rich import print
from testcontainers.core.generic import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

BACKEND_PORT = 6321
METRICS_PORT = 6322
BUCKET_NAME = "test-bucket"
DB_PATH = "db.sqlite3"
GARAGE_S3_PORT = 3900
GARAGE_ADMIN_PORT = 3903
GARAGE_CONFIG_PATH = "/etc/garage.toml"
GARAGE_IMAGE = "dxflrs/garage:v2.3.0"
GARAGE_DEFAULT_ACCESS_KEY = "testtest"
GARAGE_DEFAULT_SECRET_KEY = "test-secret-1234567890"
GARAGE_REGION = "us-east-1"

CONFIG_TEMPLATE = """
listen = "127.0.0.1:{{backend_port}}"
metrics_listen = "127.0.0.1:{{metrics_port}}"
db_path = "{{db_path}}"

{{upstreams}}

{{tiering_rules}}
"""

UPSTREAM_TEMPLATE = """
[upstreams.{{name}}]
order = {{order}}
base_url = "http://127.0.0.1:{{port}}"
addressing_style = "path"
s3_access_key = "{{s3_access_key}}"
s3_secret = "{{s3_secret}}"
region = "{{s3_region}}"
"""


class Tier(StrEnum):
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"

    def max_age_seconds(self) -> int | None:
        match self:
            case Tier.HOT:
                return 1 * 60 * 60
            case Tier.WARM:
                return 5 * 60 * 60
            case Tier.COLD:
                return None

    def order(self) -> int:
        match self:
            case Tier.HOT:
                return 1
            case Tier.WARM:
                return 2
            case Tier.COLD:
                return 3

    @staticmethod
    def all() -> list["Tier"]:
        return [Tier.HOT, Tier.WARM, Tier.COLD]


@dataclass
class Upstream:
    tier: Tier
    container: DockerContainer
    client: S3Client
    buckets: list[str]

    @staticmethod
    def create(
        tier: Tier, container: DockerContainer, buckets: list[str]
    ) -> "Upstream":
        client = boto3.client(
            "s3",
            endpoint_url=f"http://localhost:{container.get_exposed_port(GARAGE_S3_PORT)}",
            aws_access_key_id=GARAGE_DEFAULT_ACCESS_KEY,
            aws_secret_access_key=GARAGE_DEFAULT_SECRET_KEY,
        )
        for bucket in buckets:
            if bucket != BUCKET_NAME:
                client.create_bucket(Bucket=bucket)

        return Upstream(
            tier=tier,
            container=container,
            client=client,
            buckets=buckets,
        )

    def get_object_keys(self) -> list[str]:
        all_keys = []
        for bucket in self.buckets:
            all_keys.extend(self.get_object_keys_bucket(bucket))
        return all_keys

    def get_object_keys_bucket(self, bucket: str) -> list[str]:
        response = self.client.list_objects_v2(Bucket=bucket)
        return [cast(str, obj.get("Key")) for obj in response.get("Contents", [])]


@dataclass
class Backend:
    process: subprocess.Popen
    client: S3Client
    db_path: Path
    sqlite_connection: sqlite3.Connection

    def update_object_age(self, bucket: str, key: str, age_seconds: int) -> None:
        last_modified = int(time.time() * 1000) - age_seconds * 1000
        res = self.sqlite_connection.execute(
            "UPDATE objects SET last_modified = ? WHERE key = ? AND bucket = ?",
            (last_modified, key, bucket),
        )
        self.sqlite_connection.commit()
        assert res.rowcount == 1, (
            f"Expected to update exactly one row for {bucket}/{key}, but updated {res.rowcount} rows"
        )

    def assigned_upstream_counts(self) -> dict[str, int]:
        res = self.sqlite_connection.execute(
            """
            SELECT assigned_upstream, COUNT(*)
            FROM objects
            GROUP BY assigned_upstream
            """,
        ).fetchall()
        return {row[0]: row[1] for row in res}

    def assigned_upstream_for(self, bucket: str, key: str) -> str | None:
        row = self.sqlite_connection.execute(
            "SELECT assigned_upstream FROM objects WHERE bucket = ? AND key = ?",
            (bucket, key),
        ).fetchone()
        return None if row is None else cast(str, row[0])

    def access_count_for(self, bucket: str, key: str) -> int:
        row = self.sqlite_connection.execute(
            "SELECT COALESCE(SUM(count), 0) FROM AccessCounters WHERE obj_bucket = ? AND obj_key = ?",
            (bucket, key),
        ).fetchone()
        return 0 if row is None else cast(int, row[0])

    def add_access_count_days_back(
        self, bucket: str, key: str, *, days_back: int, count: int
    ) -> None:
        assert days_back >= 0, "days_back must be non-negative"
        assert count > 0, "count must be positive"

        today_local = (
            datetime.now()
            .astimezone()
            .replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
        )
        bucket_start = today_local - timedelta(days=days_back)
        time_bucket = int(bucket_start.timestamp() * 1000)

        self.sqlite_connection.execute(
            """
            INSERT INTO AccessCounters
                (obj_bucket, obj_key, time_bucket, count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (obj_bucket, obj_key, time_bucket)
            DO UPDATE SET
                count = count + excluded.count
            """,
            (bucket, key, time_bucket, count),
        )
        self.sqlite_connection.commit()

    def object_sizes(self) -> dict[tuple[str, str], int]:
        res = self.sqlite_connection.execute(
            """
            SELECT bucket, key, size
            FROM objects
            """,
        ).fetchall()
        return {(row[0], row[1]): row[2] for row in res}

    def get_current_migration_run_num(self) -> int:
        response = requests.get(
            f"http://localhost:{METRICS_PORT}/metrics", timeout=10
        ).text
        lines = [
            line
            for line in response.splitlines()
            if line.startswith("migration_runs_total")
        ]
        if lines:
            return int(lines[0].split()[-1])
        return 0

    def wait_for_tier_changes(self, *, timeout_seconds: int = 5 * 60) -> None:
        initial_migration_run = self.get_current_migration_run_num()
        deadline = time.time() + timeout_seconds
        current_assignments = self.assigned_upstream_counts()
        while True:
            time.sleep(10)
            new_assignments = self.assigned_upstream_counts()
            if new_assignments != current_assignments:
                break
            info(
                "Waiting... Current assignments: "
                + ", ".join(f"{k}: {v}" for k, v in new_assignments.items()),
                level=4,
            )

            if time.time() > deadline:
                error("Timed out waiting for tier changes to be processed")
                raise TimeoutError("Timed out waiting for tier changes to be processed")

        while initial_migration_run == self.get_current_migration_run_num():
            info(
                f"Waiting for pending migrations to be processed (run {self.get_current_migration_run_num()})...",
                level=4,
            )
            time.sleep(2)

    def wait_for_assigned_upstream(
        self,
        *,
        bucket: str,
        key: str,
        expected: Tier,
        timeout_seconds: int = 6 * 60,
    ) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            assigned = self.assigned_upstream_for(bucket, key)
            if assigned == expected.value:
                return
            info(
                f"Waiting for {bucket}/{key} to move to {expected.value}, currently {assigned}",
                level=4,
            )
            time.sleep(5)

        assigned = self.assigned_upstream_for(bucket, key)
        raise TimeoutError(
            f"Timed out waiting for {bucket}/{key} to move to {expected.value} (currently {assigned})"
        )


def _log(kind: str, message: str, *, level: int = 0) -> None:
    indent = " " * max(0, level)
    now_str = time.strftime("%M:%S")
    print(f"[dim white]{now_str}[/] {kind} {indent}{message}")


def info(message: str, *, level: int = 0) -> None:
    _log("[cyan]\\[INFO][/]", message, level=level)


def error(message: str, *, level: int = 0) -> None:
    _log("[bold red]\\[ERROR][/]", message, level=level)


def warn(message: str, *, level: int = 0) -> None:
    _log("[bold yellow]\\[WARN][/]", message, level=level)


@contextlib.contextmanager
def create_garage_container(name: str):
    with TemporaryDirectory(prefix=f"garage-{name}-") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        config_path = temp_dir / "garage.toml"
        config_path.write_text(
            f"""
metadata_dir = "/tmp/meta"
data_dir     = "/tmp/data"
db_engine    = "sqlite"

replication_factor = 1

rpc_bind_addr   = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

[s3_api]
s3_region     = "{GARAGE_REGION}"
api_bind_addr = "[::]:3900"
root_domain   = ".localhost"

[s3_web]
bind_addr   = "[::]:3902"
root_domain = ".localhost"
index       = "index.html"

[admin]
api_bind_addr = "[::]:3903"
admin_token   = "unused-for-e2e"
""".strip()
            + "\n"
        )

        container = (
            DockerContainer(GARAGE_IMAGE)
            .with_volume_mapping(config_path, GARAGE_CONFIG_PATH, "ro")
            .with_exposed_ports(GARAGE_S3_PORT, GARAGE_ADMIN_PORT)
            .with_env("GARAGE_DEFAULT_ACCESS_KEY", GARAGE_DEFAULT_ACCESS_KEY)
            .with_env("GARAGE_DEFAULT_SECRET_KEY", GARAGE_DEFAULT_SECRET_KEY)
            .with_env("GARAGE_DEFAULT_BUCKET", BUCKET_NAME)
            .with_command(
                [
                    "/garage",
                    "server",
                    "--single-node",
                    "--default-access-key",
                    "--default-bucket",
                ]
            )
            .waiting_for(
                HttpWaitStrategy(GARAGE_ADMIN_PORT, path="/health").for_status_code(200)
            )
        )
        with container:
            yield container


def render_config(
    temp_dir: Path,
    upstreams: list[Upstream],
    tiering_rules: list[tuple[Tier, str]],
) -> str:
    upstream_sections = []
    for upstream in upstreams:
        port = upstream.container.get_exposed_port(GARAGE_S3_PORT)
        upstream_section = (
            UPSTREAM_TEMPLATE.replace("{{name}}", upstream.tier.value)
            .replace("{{order}}", str(upstream.tier.order()))
            .replace("{{port}}", str(port))
            .replace("{{s3_access_key}}", GARAGE_DEFAULT_ACCESS_KEY)
            .replace("{{s3_secret}}", GARAGE_DEFAULT_SECRET_KEY)
            .replace("{{s3_region}}", GARAGE_REGION)
        )
        upstream_sections.append(upstream_section)

    tiering_rules_config = "\n\n".join(
        "\n".join(
            [
                "[[tiering_rules]]",
                f'to = "{to.value}"',
                f'when = "{when}"',
            ]
        )
        for to, when in tiering_rules
    )

    config = (
        CONFIG_TEMPLATE.replace("{{db_path}}", f"{temp_dir.absolute()}/{DB_PATH}")
        .replace("{{backend_port}}", str(BACKEND_PORT))
        .replace("{{metrics_port}}", str(METRICS_PORT))
    )
    config = config.replace("{{upstreams}}", "\n\n".join(upstream_sections))
    config = config.replace("{{tiering_rules}}", tiering_rules_config)
    return config


@contextlib.contextmanager
def start_backend(temp_dir: Path, config_path: Path):
    repo_root = Path(__file__).resolve().parents[1]

    info("Building backend...")
    subprocess.check_call(["cargo", "build", "--release"])

    logfile = temp_dir / "backend.log"
    backend_process = subprocess.Popen(
        ["cargo", "run", "--release", "--", "--config", str(config_path), "serve"],
        cwd=repo_root,
        stdout=logfile.open("w"),
        stderr=subprocess.STDOUT,
        text=True,
    )

    info(f"Starting backend with PID {backend_process.pid}, logging to {logfile}")
    info("Waiting for backend to be ready...", level=2)
    for _ in range(10):
        try:
            requests.head(f"http://localhost:{BACKEND_PORT}", timeout=10)
            break
        except requests.ConnectionError:
            info("Backend not ready yet, retrying...", level=2)
            if logfile.read_text().strip():
                for line in logfile.read_text().splitlines():
                    info(line, level=4)
            time.sleep(1)
    else:
        error("Backend did not become ready in time, check the logs for details")
        if logfile.read_text().strip():
            for line in logfile.read_text().splitlines():
                error(line, level=4)
        raise RuntimeError("Backend did not become ready in time")

    client = boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{BACKEND_PORT}",
        aws_access_key_id=GARAGE_DEFAULT_ACCESS_KEY,
        aws_secret_access_key=GARAGE_DEFAULT_SECRET_KEY,
    )
    try:
        yield Backend(
            backend_process,
            client,
            temp_dir / DB_PATH,
            sqlite3.connect(temp_dir / DB_PATH),
        )
    finally:
        if backend_process.poll() is None:
            backend_process.terminate()
            try:
                backend_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                warn("Backend did not terminate in time, killing it")
                backend_process.kill()

    has_errors = False
    for line in logfile.read_text().splitlines():
        if ("WARN" in line or "ERROR" in line) and "SIGTERM" not in line:
            has_errors = True

    if has_errors:
        warn("Backend emitted warnings during the test, check the logs for details")
        print("[dim]============== Backend logs ==============[/]")
        for line in logfile.read_text():
            sys.stdout.write(line)
        print("[dim]============== Backend logs ==============[/]")
        raise RuntimeError("Backend emitted warnings during the test")
