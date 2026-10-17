import os
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
from botocore.config import Config

from e2e_utils import (
    BACKEND_PORT,
    BUCKET_NAME,
    GARAGE_DEFAULT_ACCESS_KEY,
    GARAGE_DEFAULT_SECRET_KEY,
    Backend,
    Tier,
    Upstream,
    create_garage_container,
    info,
    render_config,
    start_backend,
)

FIVE_MIB = 5 * 1024 * 1024


def _multipart_upload(backend: Backend, *, key: str, parts: list[bytes]) -> None:
    upload_id = backend.client.create_multipart_upload(Bucket=BUCKET_NAME, Key=key)[
        "UploadId"
    ]
    completed = []
    for number, part in enumerate(parts, start=1):
        response = backend.client.upload_part(
            Bucket=BUCKET_NAME,
            Key=key,
            UploadId=upload_id,
            PartNumber=number,
            Body=part,
        )
        completed.append({"ETag": response["ETag"], "PartNumber": number})

    listed = backend.client.list_parts(Bucket=BUCKET_NAME, Key=key, UploadId=upload_id)
    assert [p["PartNumber"] for p in listed.get("Parts", [])] == list(
        range(1, len(parts) + 1)
    ), f"Unexpected parts listed for {key}: {listed.get('Parts')}"

    backend.client.complete_multipart_upload(
        Bucket=BUCKET_NAME,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": completed},
    )


def _assert_stored(
    backend: Backend, upstreams: list[Upstream], *, key: str, content: bytes
) -> None:
    body = backend.client.get_object(Bucket=BUCKET_NAME, Key=key)["Body"].read()
    assert body == content, f"Unexpected content for {key}"

    head = backend.client.head_object(Bucket=BUCKET_NAME, Key=key)
    assert head["ContentLength"] == len(content), f"Unexpected HEAD length for {key}"

    size = backend.object_sizes().get((BUCKET_NAME, key))
    assert size == len(content), (
        f"Unexpected recorded size for {key}: expected {len(content)}, got {size}"
    )

    assigned = backend.assigned_upstream_for(BUCKET_NAME, key)
    holders = [
        u.tier.value for u in upstreams if key in u.get_object_keys_bucket(BUCKET_NAME)
    ]
    assert holders == [assigned], (
        f"Expected {key} only on assigned upstream {assigned}, found on {holders}"
    )


def _assert_no_pending_uploads(upstreams: list[Upstream]) -> None:
    for upstream in upstreams:
        uploads = upstream.client.list_multipart_uploads(Bucket=BUCKET_NAME).get(
            "Uploads", []
        )
        assert not uploads, f"Leftover uploads on {upstream.tier.value}: {uploads}"


def _test_complete(backend: Backend, upstreams: list[Upstream]) -> None:
    key = "multipart-complete"
    parts = [os.urandom(FIVE_MIB), os.urandom(FIVE_MIB), os.urandom(1024)]
    _multipart_upload(backend, key=key, parts=parts)
    _assert_stored(backend, upstreams, key=key, content=b"".join(parts))


def _test_single_small_part(backend: Backend, upstreams: list[Upstream]) -> None:
    key = "multipart-single-small"
    parts = [b"tiny multipart"]
    _multipart_upload(backend, key=key, parts=parts)
    _assert_stored(backend, upstreams, key=key, content=b"".join(parts))


def _test_overwrite_existing(backend: Backend, upstreams: list[Upstream]) -> None:
    key = "multipart-overwrite"
    backend.client.put_object(Bucket=BUCKET_NAME, Key=key, Body=b"small original")
    _assert_stored(backend, upstreams, key=key, content=b"small original")

    parts = [os.urandom(FIVE_MIB), os.urandom(4096)]
    _multipart_upload(backend, key=key, parts=parts)
    _assert_stored(backend, upstreams, key=key, content=b"".join(parts))


def _test_abort(backend: Backend, upstreams: list[Upstream]) -> None:
    key = "multipart-abort"
    upload_id = backend.client.create_multipart_upload(Bucket=BUCKET_NAME, Key=key)[
        "UploadId"
    ]
    backend.client.upload_part(
        Bucket=BUCKET_NAME,
        Key=key,
        UploadId=upload_id,
        PartNumber=1,
        Body=os.urandom(1024),
    )
    backend.client.abort_multipart_upload(
        Bucket=BUCKET_NAME, Key=key, UploadId=upload_id
    )

    try:
        backend.client.get_object(Bucket=BUCKET_NAME, Key=key)
    except backend.client.exceptions.NoSuchKey:
        pass
    else:
        raise AssertionError(f"Expected {key} to be missing after abort")

    assert backend.assigned_upstream_for(BUCKET_NAME, key) is None, (
        f"Aborted upload {key} left a DB row"
    )
    _assert_no_pending_uploads(upstreams)


def _test_managed_transfer(backend: Backend, upstreams: list[Upstream]) -> None:
    key = "multipart-managed"
    # Garage reports composite multipart CRCs as full-object checksums, which
    # boto3 then fails to validate on GET.
    # https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/1228
    client = boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{BACKEND_PORT}",
        aws_access_key_id=GARAGE_DEFAULT_ACCESS_KEY,
        aws_secret_access_key=GARAGE_DEFAULT_SECRET_KEY,
        config=Config(request_checksum_calculation="when_required"),
    )
    with TemporaryDirectory(prefix="multipart-src-") as src_dir:
        src = Path(src_dir) / "payload"
        content = os.urandom(3 * FIVE_MIB + 17)
        src.write_bytes(content)
        client.upload_file(str(src), BUCKET_NAME, key)
    _assert_stored(backend, upstreams, key=key, content=content)


def main() -> None:
    info("Starting multipart e2e test...")

    rules = [
        (Tier.HOT, "size < 1MiB"),
        (Tier.COLD, "true"),
    ]

    with (
        create_garage_container("hot") as hot_container,
        create_garage_container("cold") as cold_container,
        TemporaryDirectory(prefix="multipart-e2e-") as temp_dir_str,
    ):
        info("Initializing upstreams...", level=2)
        buckets = [BUCKET_NAME]
        hot = Upstream.create(Tier.HOT, hot_container, buckets)
        cold = Upstream.create(Tier.COLD, cold_container, buckets)
        upstreams = [hot, cold]

        temp_dir = Path(temp_dir_str)
        config_path = temp_dir / "config.toml"
        config_path.write_text(render_config(temp_dir, upstreams, rules))

        with start_backend(temp_dir, config_path) as backend:
            for name, test in [
                ("complete", _test_complete),
                ("single small part", _test_single_small_part),
                ("overwrite existing", _test_overwrite_existing),
                ("abort", _test_abort),
                ("managed transfer", _test_managed_transfer),
            ]:
                info(f"Running multipart scenario: {name}", level=2)
                test(backend, upstreams)
            _assert_no_pending_uploads(upstreams)

    info("Multipart e2e test completed successfully!")
