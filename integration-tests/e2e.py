# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "boto3>=1.42.69",
#     "boto3-stubs[s3]>=1.42.70",
#     "rich>=14.3.3",
#     "testcontainers>=4.14.1",
# ]
# ///

import contextlib
import random
import sqlite3
import subprocess
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from typing import cast

import boto3
import requests
from mypy_boto3_s3 import S3Client
from rich import print
from rich.progress import track
from testcontainers.core.generic import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

BACKEND_PORT = 6321
BUCKET_NAME = "test-bucket"
DB_PATH = "db.sqlite3"

CONFIG_TEMPLATE = """
listen = "127.0.0.1:{{backend_port}}"
db_path = "{{db_path}}"

{{upstreams}}
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
        # We will be running for quite a bit, so use at least an hour of difference
        match self:
            case Tier.HOT:
                return 1 * 60 * 60
            case Tier.WARM:
                return 5 * 60 * 60
            case Tier.COLD:
                return None

    def random_age_seconds(self) -> int:
        safety_buffer = 10 * 60  # 10 minutes buffer to avoid edge cases
        match self:
            case Tier.HOT:
                return random.randint(
                    0, cast(int, self.max_age_seconds()) - safety_buffer
                )
            case Tier.WARM:
                return random.randint(
                    cast(int, Tier.HOT.max_age_seconds()) + safety_buffer,
                    cast(int, self.max_age_seconds()) - safety_buffer,
                )
            case Tier.COLD:
                return random.randint(
                    cast(int, Tier.WARM.max_age_seconds()) + safety_buffer, 24 * 60 * 60
                )

    def order(self) -> int:
        match self:
            case Tier.HOT:
                return 1
            case Tier.WARM:
                return 2
            case Tier.COLD:
                return 3

    @staticmethod
    def random() -> "Tier":
        return random.choice(Tier.all())

    @staticmethod
    def all() -> list["Tier"]:
        return [Tier.HOT, Tier.WARM, Tier.COLD]


@dataclass
class Upstream:
    tier: Tier
    container: DockerContainer
    client: S3Client

    @staticmethod
    def create(tier: Tier, container: DockerContainer) -> "Upstream":
        client = boto3.client(
            "s3",
            endpoint_url=f"http://localhost:{container.get_exposed_port(9090)}",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        client.create_bucket(Bucket=BUCKET_NAME)

        return Upstream(
            tier=tier,
            container=container,
            client=client,
        )

    def get_object_keys(self) -> list[str]:
        response = self.client.list_objects_v2(Bucket=BUCKET_NAME)
        return [cast(str, obj.get("Key")) for obj in response.get("Contents", [])]


@dataclass
class Backend:
    process: subprocess.Popen
    client: S3Client
    db_path: Path
    sqlite_connection: sqlite3.Connection

    def update_object_age(self, object: "S3TestObject") -> None:
        timey = int(time.time() * 1000) - object.age_seconds * 1000
        res = self.sqlite_connection.execute(
            "UPDATE objects SET last_modified = ? WHERE key = ? AND bucket = ?",
            (
                timey,
                object.key,
                BUCKET_NAME,
            ),
        )
        self.sqlite_connection.commit()
        assert res.rowcount == 1, (
            f"Expected to update exactly one row for object {object.key}, but updated {res.rowcount} rows"
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

    def pending_migrations_count(self) -> int:
        res = self.sqlite_connection.execute(
            "SELECT COUNT(*) FROM PendingMigrations"
        ).fetchone()
        return res[0]

    def wait_for_tier_changes(self):
        deadline = (
            time.time() + 5 * 60
        )  # 5 minutes timeout to avoid infinite loops in case of issues
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

        while self.pending_migrations_count() > 0:
            info(
                f"Waiting for pending migrations to be processed ({self.pending_migrations_count()} remaining)...",
                level=4,
            )
            time.sleep(2)


@dataclass
class S3TestObject:
    key: str
    content: bytes
    age_seconds: int
    tier: Tier

    @staticmethod
    def new_random(key: str) -> "S3TestObject":
        import random

        payload_length = random.randint(1, 1024 * 1024)  # Up to 1 MiB
        random_bytes = random.randbytes(payload_length)
        return S3TestObject(
            key=key,
            content=random_bytes,
            age_seconds=0,
            tier=Tier.HOT,
        )


@dataclass
class TestData:
    objects: list[S3TestObject]
    backend: Backend
    hot: Upstream
    warm: Upstream
    cold: Upstream

    def all_keys(self) -> list[str]:
        return [obj.key for obj in self.objects]

    def assert_all_hot(self):
        hot_keys = self.hot.get_object_keys()
        expected_keys = self.all_keys()
        assert set(hot_keys) == set(expected_keys), (
            f"Expected all objects to be in the hot tier, but got {len(hot_keys)} hot objects and {len(expected_keys)} expected objects"
        )
        assert len(self.warm.get_object_keys()) == 0, (
            f"Expected no objects in the warm tier, but got {len(self.warm.get_object_keys())} objects"
        )
        assert len(self.cold.get_object_keys()) == 0, (
            f"Expected no objects in the cold tier, but got {len(self.cold.get_object_keys())} objects"
        )

    def randomize_object_tiers(self):
        for obj in track(
            self.objects, description="Randomizing object tiers...", transient=True
        ):
            obj.tier = Tier.random()
            new_age = obj.tier.random_age_seconds()
            obj.age_seconds = new_age
            self.backend.update_object_age(obj)

    def assert_tiers_match(self):
        upstream_keys = {
            "hot": set(self.hot.get_object_keys()),
            "warm": set(self.warm.get_object_keys()),
            "cold": set(self.cold.get_object_keys()),
        }
        for obj in track(
            self.objects, description="Verifying object tiers...", transient=True
        ):
            for tier in Tier.all():
                if obj.key in upstream_keys[tier.value]:
                    assert tier == obj.tier, (
                        f"Object {obj.key} is in the {tier.value} tier but expected to be in {obj.tier.value} tier"
                    )

            from_backend = (
                self.backend.client.get_object(Bucket=BUCKET_NAME, Key=obj.key)
                .get("Body")
                .read()
            )
            from_upstream = (
                self._upstream_for(obj.tier)
                .client.get_object(Bucket=BUCKET_NAME, Key=obj.key)
                .get("Body")
                .read()
            )
            assert obj.content == from_backend == from_upstream, (
                f"Object content mismatch for {obj.key}"
            )

    def _upstream_for(self, tier: Tier) -> Upstream:
        match tier:
            case Tier.HOT:
                return self.hot
            case Tier.WARM:
                return self.warm
            case Tier.COLD:
                return self.cold

    @staticmethod
    def create_and_upload(
        backend: Backend, hot: Upstream, warm: Upstream, cold: Upstream, count: int
    ) -> "TestData":
        objects = []
        for index in track(
            range(count), description="Uploading test objects...", transient=True
        ):
            object = S3TestObject.new_random(f"object-{index}")
            backend.client.put_object(
                Bucket=BUCKET_NAME,
                Key=object.key,
                Body=object.content,
            )
            objects.append(object)

        return TestData(objects, backend, hot, warm, cold)


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


def main() -> None:
    info("Starting test...")

    info("Creating S3Mock upstreams...")
    with (
        create_s3mock_container() as hot_container,
        create_s3mock_container() as warm_container,
        create_s3mock_container() as cold_container,
        TemporaryDirectory(prefix="tiering-e2e-") as temp_dir_str,
    ):
        info("Initializing upstreams...", level=2)
        hot = Upstream.create(Tier.HOT, hot_container)
        warm = Upstream.create(Tier.WARM, warm_container)
        cold = Upstream.create(Tier.COLD, cold_container)
        temp_dir = Path(temp_dir_str)

        config = render_config(temp_dir, [hot, warm, cold])
        config_path = temp_dir / "config.toml"
        config_path.write_text(config)
        with start_backend(temp_dir, config_path) as backend:
            info("Uploading test data...")
            test_data = TestData.create_and_upload(backend, hot, warm, cold, count=500)

            info("Verifying all objects are in the hot tier initially...", level=2)
            test_data.assert_all_hot()

            for randomize_round in range(3):
                info(f"Randomizing object tiers (round {randomize_round + 1}/3)")
                test_data.randomize_object_tiers()
                info("Wait for backend to process tier changes...", level=2)
                backend.wait_for_tier_changes()
                info("Verifying object tiers match expected tiers...", level=2)
                test_data.assert_tiers_match()

            info("Test completed successfully!")


def create_s3mock_container() -> DockerContainer:
    return (
        DockerContainer("adobe/s3mock:latest")
        .with_exposed_ports(9090)
        .waiting_for(HttpWaitStrategy(9090).for_status_code(200))
    )


def render_config(temp_dir: Path, started_containers: list[Upstream]):
    upstreams = []
    for upstream in started_containers:
        port = upstream.container.get_exposed_port(9090)
        upstream_config = (
            UPSTREAM_TEMPLATE.replace("{{name}}", upstream.tier.value)
            .replace("{{order}}", str(upstream.tier.order()))
            .replace("{{port}}", str(port))
            .replace("{{s3_access_key}}", "test")
            .replace("{{s3_secret}}", "test")
            .replace("{{s3_region}}", "us-east-1")
        )
        if upstream.tier.max_age_seconds() is not None:
            upstream_config += f'max_age = "{upstream.tier.max_age_seconds()}s"'

        upstreams.append(upstream_config)

    config = CONFIG_TEMPLATE.replace(
        "{{db_path}}", f"{temp_dir.absolute()}/{DB_PATH}"
    ).replace("{{backend_port}}", str(BACKEND_PORT))
    config = config.replace("{{upstreams}}", "\n\n".join(upstreams))

    return config


@contextlib.contextmanager
def start_backend(temp_dir: Path, config_path: Path):
    repo_root = Path(__file__).resolve().parents[1]
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
            requests.head(f"http://localhost:{BACKEND_PORT}")
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
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    yield Backend(
        backend_process,
        client,
        temp_dir / DB_PATH,
        sqlite3.connect(temp_dir / DB_PATH),
    )

    if backend_process.poll() is None:
        backend_process.terminate()
        try:
            backend_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            warn("Backend did not terminate in time, killing it")
            backend_process.kill()


if __name__ == "__main__":
    main()
