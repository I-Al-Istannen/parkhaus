from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from hypothesis import HealthCheck, Verbosity, settings, strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
    run_state_machine_as_test,
)

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

ONE_MIB = 1024 * 1024
MAX_TRACKED_OBJECTS = 100


def _build_payload(*, seed: bytes, is_large: bool, large_extra: int) -> bytes:
    if not is_large:
        return b"small-" + seed

    target_length = ONE_MIB + large_extra
    repeats = (target_length + len(seed) - 1) // len(seed)
    return (seed * repeats)[:target_length]


def _assert_get_matches(backend: Backend, *, key: str, expected_content: bytes) -> None:
    body = backend.client.get_object(Bucket=BUCKET_NAME, Key=key).get("Body").read()
    assert body == expected_content, f"Unexpected object content after GET for {key}"


def _assert_recorded_size(backend: Backend, *, key: str, expected_size: int) -> None:
    sizes = backend.object_sizes()
    object_id = (BUCKET_NAME, key)
    assert object_id in sizes, f"Missing DB row for {key}"
    assert sizes[object_id] == expected_size, (
        f"Unexpected recorded object size for {key}: expected {expected_size}, got {sizes[object_id]}"
    )


def _assert_missing_key(backend: Backend, *, key: str) -> None:
    try:
        backend.client.get_object(Bucket=BUCKET_NAME, Key=key)
    except backend.client.exceptions.NoSuchKey:
        return
    raise AssertionError(f"Expected {key} to be missing")


def _run_hypothesis_crud_checks(backend: Backend) -> None:
    @settings(
        max_examples=50,
        stateful_step_count=30,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
        verbosity=Verbosity.normal,
    )
    class CrudStateMachine(RuleBasedStateMachine):
        @initialize()
        def init_model(self) -> None:
            self.example_prefix = f"crud-hyp-{uuid4().hex}"
            self.next_key_index = 0
            self.live_objects: dict[str, bytes] = {}
            self.deleted_objects: set[str] = set()

        def teardown(self) -> None:
            for key in list(self.live_objects):
                backend.client.delete_object(Bucket=BUCKET_NAME, Key=key)

        def _new_key(self, suffix: str) -> str:
            key = f"{self.example_prefix}-{suffix}-{self.next_key_index}"
            self.next_key_index += 1
            return key

        def _assert_live_object(self, key: str) -> None:
            expected_content = self.live_objects[key]
            _assert_get_matches(backend, key=key, expected_content=expected_content)
            _assert_recorded_size(backend, key=key, expected_size=len(expected_content))

        @rule(
            is_large=st.booleans(),
            large_extra=st.integers(min_value=1, max_value=128 * 1024),
            seed=st.binary(min_size=1, max_size=64),
        )
        @precondition(lambda self: len(self.live_objects) < MAX_TRACKED_OBJECTS)
        def put_new_object(self, is_large: bool, large_extra: int, seed: bytes) -> None:
            key = self._new_key("obj")
            payload = _build_payload(
                seed=seed,
                is_large=is_large,
                large_extra=large_extra,
            )

            backend.client.put_object(Bucket=BUCKET_NAME, Key=key, Body=payload)

            self.live_objects[key] = payload
            self.deleted_objects.discard(key)
            self._assert_live_object(key)

        @rule(
            data=st.data(),
            is_large=st.booleans(),
            large_extra=st.integers(min_value=1, max_value=128 * 1024),
            seed=st.binary(min_size=1, max_size=64),
        )
        @precondition(lambda self: bool(self.live_objects))
        def overwrite_existing_object(
            self,
            data: st.DataObject,
            is_large: bool,
            large_extra: int,
            seed: bytes,
        ) -> None:
            key = data.draw(st.sampled_from(tuple(self.live_objects.keys())))
            payload = _build_payload(
                seed=seed,
                is_large=is_large,
                large_extra=large_extra,
            )

            backend.client.put_object(Bucket=BUCKET_NAME, Key=key, Body=payload)
            self.live_objects[key] = payload
            self._assert_live_object(key)

        @rule(data=st.data())
        @precondition(lambda self: bool(self.live_objects))
        def get_existing_object(self, data: st.DataObject) -> None:
            key = data.draw(st.sampled_from(tuple(self.live_objects.keys())))
            self._assert_live_object(key)

        @rule(data=st.data())
        @precondition(lambda self: bool(self.live_objects))
        def head_existing_object(self, data: st.DataObject) -> None:
            key = data.draw(st.sampled_from(tuple(self.live_objects.keys())))
            response = backend.client.head_object(Bucket=BUCKET_NAME, Key=key)
            assert response["ContentLength"] == len(self.live_objects[key]), (
                f"Unexpected content length for {key}"
            )

        @rule(data=st.data())
        @precondition(lambda self: bool(self.live_objects))
        def delete_existing_object(self, data: st.DataObject) -> None:
            key = data.draw(st.sampled_from(tuple(self.live_objects.keys())))
            backend.client.delete_object(Bucket=BUCKET_NAME, Key=key)

            self.live_objects.pop(key)
            self.deleted_objects.add(key)
            _assert_missing_key(backend, key=key)

        @rule(data=st.data())
        def get_missing_object(self, data: st.DataObject) -> None:
            use_deleted = bool(self.deleted_objects) and data.draw(st.booleans())
            if use_deleted:
                key = data.draw(st.sampled_from(tuple(self.deleted_objects)))
            else:
                key = self._new_key("missing")

            _assert_missing_key(backend, key=key)

        @invariant()
        def live_objects_are_always_readable(self) -> None:
            for key in sorted(self.live_objects):
                self._assert_live_object(key)

    run_state_machine_as_test(CrudStateMachine)


def main() -> None:
    info("Starting crud e2e test...")

    rules = [
        (Tier.HOT, "size < 1MiB"),
        (Tier.COLD, "true"),
    ]

    with (
        create_garage_container("hot") as hot_container,
        create_garage_container("cold") as cold_container,
        TemporaryDirectory(prefix="crud-e2e-") as temp_dir_str,
    ):
        info("Initializing upstreams...", level=2)
        buckets = [BUCKET_NAME]
        hot = Upstream.create(Tier.HOT, hot_container, buckets)
        cold = Upstream.create(Tier.COLD, cold_container, buckets)

        temp_dir = Path(temp_dir_str)
        config = render_config(temp_dir, [hot, cold], rules)
        config_path = temp_dir / "config.toml"
        config_path.write_text(config)

        with start_backend(temp_dir, config_path) as backend:
            info("Running Hypothesis stateful CRUD checks...", level=2)
            _run_hypothesis_crud_checks(backend)

    info("Crud e2e test completed successfully!")
