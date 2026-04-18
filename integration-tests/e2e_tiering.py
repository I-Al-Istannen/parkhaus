import random
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from rich.progress import track

from e2e_utils import (
    BUCKET_NAME,
    Backend,
    Tier,
    Upstream,
    create_garage_container,
    info,
    render_config,
    start_backend,
)


@dataclass
class S3TestObject:
    key: str
    bucket: str
    content: bytes
    age_seconds: int
    tier: Tier

    @staticmethod
    def new_random(key: str, bucket: str) -> "S3TestObject":
        payload_length = random.randint(1, 1024 * 1024)
        return S3TestObject(
            key=key,
            bucket=bucket,
            content=random.randbytes(payload_length),
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
    buckets_stop_at_warm: set[str]

    def all_keys(self) -> list[str]:
        return [obj.key for obj in self.objects]

    def assert_all_hot(self) -> None:
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

    def randomize_object_tiers(self) -> None:
        for obj in track(
            self.objects, description="Randomizing object tiers...", transient=True
        ):
            obj.tier = random.choice(Tier.all())
            obj.age_seconds = random_age_for_tier(obj.tier)
            self.backend.update_object_age(obj.bucket, obj.key, obj.age_seconds)

            if obj.tier == Tier.COLD and obj.bucket in self.buckets_stop_at_warm:
                obj.tier = Tier.WARM

    def assert_tiers_match(self) -> None:
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
                        f"Object {obj.bucket}/{obj.key} is in the {tier.value} tier but expected to be in {obj.tier.value} tier with age {obj.age_seconds}s"
                    )

            from_backend = (
                self.backend.client.get_object(Bucket=obj.bucket, Key=obj.key)
                .get("Body")
                .read()
            )
            from_upstream = (
                self._upstream_for(obj.tier)
                .client.get_object(Bucket=obj.bucket, Key=obj.key)
                .get("Body")
                .read()
            )
            assert obj.content == from_backend == from_upstream, (
                f"Object content mismatch for {obj.bucket}/{obj.key}"
            )

    def assert_recorded_sizes_match_uploads(self) -> None:
        time.sleep(1)
        actual_sizes = self.backend.object_sizes()
        for obj in self.objects:
            object_id = (obj.bucket, obj.key)
            assert object_id in actual_sizes, (
                f"Missing object row for {obj.bucket}/{obj.key}"
            )

            expected_size = len(obj.content)
            actual_size = actual_sizes[object_id]
            assert actual_size == expected_size, (
                f"Size mismatch for {obj.bucket}/{obj.key}: expected {expected_size}, got {actual_size}"
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
        backend: Backend,
        hot: Upstream,
        warm: Upstream,
        cold: Upstream,
        buckets: list[str],
        buckets_stop_at_warm: set[str],
        count: int,
    ) -> "TestData":
        objects: list[S3TestObject] = []
        for index in track(
            range(count), description="Uploading test objects...", transient=True
        ):
            obj = S3TestObject.new_random(
                f"object-{index}",
                bucket=random.choice(buckets),
            )
            if index == 0:
                obj.content = random.randbytes(50 * 1024 * 1024)

            backend.client.put_object(
                Bucket=obj.bucket,
                Key=obj.key,
                Body=obj.content,
            )
            objects.append(obj)

        return TestData(objects, backend, hot, warm, cold, buckets_stop_at_warm)


def random_age_for_tier(tier: Tier) -> int:
    safety_buffer = 10 * 60
    hot_max = 1 * 60 * 60
    warm_max = 5 * 60 * 60
    match tier:
        case Tier.HOT:
            return random.randint(0, hot_max - safety_buffer)
        case Tier.WARM:
            return random.randint(hot_max + safety_buffer, warm_max - safety_buffer)
        case Tier.COLD:
            return random.randint(warm_max + safety_buffer, 24 * 60 * 60)


def test_get_unknown_defaults_to_coldest_upstream(
    test_data: TestData, bucket: str
) -> None:
    info("Testing GET defaults to coldest_upstream for unknown objects...", level=2)

    unknown_object = S3TestObject.new_random("unknown-object", bucket)
    test_data.cold.client.put_object(
        Bucket=bucket,
        Key=unknown_object.key,
        Body=unknown_object.content,
    )
    info("Uploaded unknown object to cold upstream only", level=4)

    hot_keys = test_data.hot.get_object_keys()
    warm_keys = test_data.warm.get_object_keys()
    cold_keys = test_data.cold.get_object_keys()

    assert unknown_object.key not in hot_keys, (
        "Unknown object should not be in hot tier"
    )
    assert unknown_object.key not in warm_keys, (
        "Unknown object should not be in warm tier"
    )
    assert unknown_object.key in cold_keys, "Unknown object should be in cold tier"
    info("Verified object is only in cold upstream", level=4)

    response = test_data.backend.client.get_object(
        Bucket=bucket,
        Key=unknown_object.key,
    )
    retrieved_content = response.get("Body").read()

    assert retrieved_content == unknown_object.content, (
        "Retrieved content should match original content for unknown object"
    )
    info(
        "Successfully retrieved unknown object through proxy from coldest upstream",
        level=4,
    )


def main(test_tier_by_bucket: bool) -> None:
    info("Starting tiering e2e test...")

    buckets = (
        [f"bucket-{i}" for i in range(10)] if test_tier_by_bucket else [BUCKET_NAME]
    )
    buckets_stop_at_warm = (
        set(random.sample(buckets, k=5)) if test_tier_by_bucket else set()
    )

    hot_max_age_seconds = 1 * 60 * 60
    warm_max_age_seconds = 5 * 60 * 60
    warm_rule = f"age <= {warm_max_age_seconds}s"
    if buckets_stop_at_warm:
        stop_at_warm_buckets = " || ".join(
            f"bucket == '{bucket}'" for bucket in sorted(buckets_stop_at_warm)
        )
        warm_rule = f"({warm_rule}) || ({stop_at_warm_buckets})"

    rules = [
        (Tier.HOT, f"age <= {hot_max_age_seconds}s"),
        (Tier.WARM, warm_rule),
        (Tier.COLD, "true"),
    ]

    info("Creating Garage upstreams...")
    with (
        create_garage_container("hot") as hot_container,
        create_garage_container("warm") as warm_container,
        create_garage_container("cold") as cold_container,
        TemporaryDirectory(prefix="tiering-e2e-") as temp_dir_str,
    ):
        info("Initializing upstreams...", level=2)
        hot = Upstream.create(Tier.HOT, hot_container, buckets)
        warm = Upstream.create(Tier.WARM, warm_container, buckets)
        cold = Upstream.create(Tier.COLD, cold_container, buckets)

        temp_dir = Path(temp_dir_str)
        config = render_config(temp_dir, [hot, warm, cold], rules)
        config_path = temp_dir / "config.toml"
        config_path.write_text(config)

        with start_backend(temp_dir, config_path) as backend:
            info("Uploading test data...")
            test_data = TestData.create_and_upload(
                backend, hot, warm, cold, buckets, buckets_stop_at_warm, count=500
            )

            info("Verifying recorded object sizes match uploaded payloads...", level=2)
            test_data.assert_recorded_sizes_match_uploads()

            info("Verifying all objects are in the hot tier initially...", level=2)
            test_data.assert_all_hot()

            info(
                "Testing proxy defaults to coldest_upstream for unknown objects...",
                level=2,
            )
            test_get_unknown_defaults_to_coldest_upstream(test_data, buckets[0])

            for randomize_round in range(3):
                info(f"Randomizing object tiers (round {randomize_round + 1}/3)")
                test_data.randomize_object_tiers()
                info("Wait for backend to process tier changes...", level=2)
                backend.wait_for_tier_changes()
                info("Verifying object tiers match expected tiers...", level=2)
                test_data.assert_tiers_match()

    info("Tiering e2e test completed successfully!")
