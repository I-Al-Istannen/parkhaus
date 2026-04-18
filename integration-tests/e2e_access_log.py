from pathlib import Path
from tempfile import TemporaryDirectory

from e2e_utils import (
    BUCKET_NAME,
    Tier,
    Upstream,
    create_garage_container,
    info,
    render_config,
    start_backend,
)


def main() -> None:
    info("Starting access-log e2e test...")

    rules = [
        (Tier.HOT, "access_counts(0d, 3d) >= 8"),
        (Tier.WARM, "access_counts(0d, 3d) >= 5"),
        (Tier.COLD, "true"),
    ]

    with (
        create_garage_container("hot") as hot_container,
        create_garage_container("warm") as warm_container,
        create_garage_container("cold") as cold_container,
        TemporaryDirectory(prefix="access-log-e2e-") as temp_dir_str,
    ):
        info("Initializing upstreams...", level=2)
        buckets = [BUCKET_NAME]
        hot = Upstream.create(Tier.HOT, hot_container, buckets)
        warm = Upstream.create(Tier.WARM, warm_container, buckets)
        cold = Upstream.create(Tier.COLD, cold_container, buckets)

        temp_dir = Path(temp_dir_str)
        config = render_config(temp_dir, [hot, warm, cold], rules)
        config_path = temp_dir / "config.toml"
        config_path.write_text(config)

        with start_backend(temp_dir, config_path) as backend:
            object_key = "access-log-target"
            content = b"hello from access-log test"

            info("Uploading object to backend...", level=2)
            backend.client.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=content)

            info("Waiting for object to age into cold tier...", level=2)
            backend.wait_for_assigned_upstream(
                bucket=BUCKET_NAME,
                key=object_key,
                expected=Tier.COLD,
            )

            info(
                "Adding synthetic historical access counters that are still below migration thresholds...",
                level=2,
            )
            backend.add_access_count_days_back(
                BUCKET_NAME,
                object_key,
                days_back=1,
                count=2,
            )
            backend.add_access_count_days_back(
                BUCKET_NAME,
                object_key,
                days_back=3,
                count=2,
            )

            info(
                "Verifying object remains in cold tier because counters are below warm/hot thresholds...",
                level=2,
            )
            backend.wait_for_assigned_upstream(
                bucket=BUCKET_NAME,
                key=object_key,
                expected=Tier.COLD,
            )

            info(
                "Requesting object to add live access counts and trigger warm migration...",
                level=2,
            )
            for _ in range(2):
                body = (
                    backend.client.get_object(Bucket=BUCKET_NAME, Key=object_key)
                    .get("Body")
                    .read()
                )
                assert body == content, "Unexpected object content after GET"

            backend.wait_for_assigned_upstream(
                bucket=BUCKET_NAME,
                key=object_key,
                expected=Tier.WARM,
            )

            info(
                "Requesting object a few more times so access counts fulfill hot rule...",
                level=2,
            )
            for _ in range(2):
                body = (
                    backend.client.get_object(Bucket=BUCKET_NAME, Key=object_key)
                    .get("Body")
                    .read()
                )
                assert body == content, "Unexpected object content after GET"

            backend.wait_for_assigned_upstream(
                bucket=BUCKET_NAME,
                key=object_key,
                expected=Tier.HOT,
            )

            hot_keys = hot.get_object_keys_bucket(BUCKET_NAME)
            warm_keys = warm.get_object_keys_bucket(BUCKET_NAME)
            cold_keys = cold.get_object_keys_bucket(BUCKET_NAME)
            assert object_key in hot_keys, "Expected final migration to hot tier"
            assert object_key not in warm_keys, "Expected object to leave warm tier"
            assert object_key not in cold_keys, "Expected object to leave cold tier"

    info("Access-log e2e test completed successfully!")
