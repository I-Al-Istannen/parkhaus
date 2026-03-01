#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "requests>=2.32.5",
# ]
# ///
"""
End-to-end bucket lifecycle test against localhost:8080.

Steps:
  1. Create a randomly named bucket (via context manager – deleted on exit).
  2. Upload a handful of files with random content.
  3. List all files and verify every uploaded key is present.
  4. Download each file and verify the content matches what was uploaded.
  5. Delete a few of the files.
  6. List again and verify the deleted keys are gone and the rest are still present.
  7. (Context-manager exit) Delete the whole bucket.
"""

import argparse
import contextlib
import logging
import os
import uuid
import xml.etree.ElementTree as ET

import requests

LOGGER = logging.getLogger("bucket-lifecycle-test")

S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _url(endpoint: str, *parts: str) -> str:
    base = endpoint.rstrip("/")
    return "/".join([base] + list(parts))


def create_bucket(endpoint: str, bucket: str) -> None:
    LOGGER.info("Creating bucket: %s", bucket)
    resp = requests.put(_url(endpoint, bucket), timeout=10)
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"create bucket failed: {resp.status_code} {resp.text}"
        )


def delete_bucket(endpoint: str, bucket: str) -> None:
    LOGGER.info("Deleting bucket: %s", bucket)
    resp = requests.delete(_url(endpoint, bucket), timeout=10)
    if resp.status_code not in (200, 204):
        raise RuntimeError(
            f"delete bucket failed: {resp.status_code} {resp.text}"
        )


def put_object(endpoint: str, bucket: str, key: str, data: bytes) -> None:
    LOGGER.info("PUT s3://%s/%s (%d bytes)", bucket, key, len(data))
    resp = requests.put(_url(endpoint, bucket, key), data=data, timeout=10)
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"put object failed for {key!r}: {resp.status_code} {resp.text}"
        )


def get_object(endpoint: str, bucket: str, key: str) -> bytes:
    LOGGER.debug("GET s3://%s/%s", bucket, key)
    resp = requests.get(_url(endpoint, bucket, key), timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(
            f"get object failed for {key!r}: {resp.status_code} {resp.text}"
        )
    return resp.content


def delete_object(endpoint: str, bucket: str, key: str) -> None:
    LOGGER.info("DELETE s3://%s/%s", bucket, key)
    resp = requests.delete(_url(endpoint, bucket, key), timeout=10)
    if resp.status_code not in (200, 204):
        raise RuntimeError(
            f"delete object failed for {key!r}: {resp.status_code} {resp.text}"
        )


def list_objects(endpoint: str, bucket: str) -> list[str]:
    """Return list of keys in the bucket via S3 ListObjects (XML response)."""
    LOGGER.debug("LIST s3://%s/", bucket)
    resp = requests.get(_url(endpoint, bucket), timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(
            f"list objects failed: {resp.status_code} {resp.text}"
        )
    root = ET.fromstring(resp.text)
    keys = [
        elem.text
        for elem in root.findall(f"{{{S3_NS}}}Contents/{{{S3_NS}}}Key")
        if elem.text is not None
    ]
    LOGGER.debug("Listed %d key(s): %s", len(keys), keys)
    return keys


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def managed_bucket(endpoint: str, bucket: str):
    """Create the bucket on entry; delete it (and everything in it) on exit."""
    create_bucket(endpoint, bucket)
    try:
        yield bucket
    finally:
        # Best-effort cleanup: remove any remaining objects before deleting the
        # bucket (some S3 implementations require an empty bucket).
        try:
            remaining = list_objects(endpoint, bucket)
            for key in remaining:
                try:
                    delete_object(endpoint, bucket, key)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("Could not delete %s during cleanup: %s", key, exc)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Could not list objects during cleanup: %s", exc)

        delete_bucket(endpoint, bucket)
        LOGGER.info("Bucket %s cleaned up", bucket)


# ---------------------------------------------------------------------------
# Test logic
# ---------------------------------------------------------------------------

def run_test(endpoint: str) -> None:
    bucket_name = f"lifecycle-test-{uuid.uuid4().hex[:10]}"
    LOGGER.info("Using bucket: %s", bucket_name)

    with managed_bucket(endpoint, bucket_name):
        # ------------------------------------------------------------------
        # 1. Upload files
        # ------------------------------------------------------------------
        files: dict[str, bytes] = {
            f"file-{i:02d}.bin": os.urandom(64 + i * 32)
            for i in range(6)
        }
        for key, data in files.items():
            put_object(endpoint, bucket_name, key, data)

        # ------------------------------------------------------------------
        # 2. List and verify all keys are present
        # ------------------------------------------------------------------
        LOGGER.info("Verifying all uploaded keys are listed …")
        listed = list_objects(endpoint, bucket_name)
        for key in files:
            assert key in listed, f"Expected key {key!r} in listing, got: {listed}"
        assert len(listed) == len(files), (
            f"Expected {len(files)} keys, listed {len(listed)}: {listed}"
        )
        LOGGER.info("Listing OK – %d key(s) found", len(listed))

        # ------------------------------------------------------------------
        # 3. Retrieve each file and verify content
        # ------------------------------------------------------------------
        LOGGER.info("Verifying content of each file …")
        for key, expected in files.items():
            actual = get_object(endpoint, bucket_name, key)
            assert actual == expected, (
                f"Content mismatch for {key!r}: "
                f"expected {len(expected)} bytes, got {len(actual)} bytes"
            )
        LOGGER.info("Content verification OK")

        # ------------------------------------------------------------------
        # 4. Delete a subset of the files
        # ------------------------------------------------------------------
        all_keys = list(files.keys())
        keys_to_delete = all_keys[:3]
        keys_to_keep = all_keys[3:]

        LOGGER.info("Deleting %d key(s): %s", len(keys_to_delete), keys_to_delete)
        for key in keys_to_delete:
            delete_object(endpoint, bucket_name, key)

        # ------------------------------------------------------------------
        # 5. List again and verify deleted keys are gone, rest still present
        # ------------------------------------------------------------------
        LOGGER.info("Verifying listing after partial deletion …")
        listed_after = list_objects(endpoint, bucket_name)

        for key in keys_to_delete:
            assert key not in listed_after, (
                f"Deleted key {key!r} still appears in listing: {listed_after}"
            )
        for key in keys_to_keep:
            assert key in listed_after, (
                f"Kept key {key!r} missing from listing after deletion: {listed_after}"
            )
        assert len(listed_after) == len(keys_to_keep), (
            f"Expected {len(keys_to_keep)} key(s) after deletion, "
            f"got {len(listed_after)}: {listed_after}"
        )
        LOGGER.info(
            "Post-deletion listing OK – %d key(s) remaining", len(listed_after)
        )

    # Context manager has now deleted the bucket.
    LOGGER.info("Bucket lifecycle test passed ✓")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="S3 bucket lifecycle test against a single endpoint"
    )
    parser.add_argument(
        "--endpoint",
        default="http://localhost:8080",
        help="Base URL of the S3-compatible endpoint (default: http://localhost:8080)",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    run_test(args.endpoint)


if __name__ == "__main__":
    main()

