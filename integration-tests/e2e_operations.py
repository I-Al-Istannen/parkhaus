"""Exercises every S3 operation Garage supports through parkhaus.

Objects are created either through parkhaus (landing on the hottest upstream)
or directly on the cold upstream, mimicking pre-existing data parkhaus does not
know about yet. Each check is independent; all results are reported at the end.
"""

import hashlib
import os
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from rich import print
from rich.table import Table

from e2e_utils import (
    BACKEND_PORT,
    BUCKET_NAME,
    GARAGE_DEFAULT_ACCESS_KEY,
    GARAGE_DEFAULT_SECRET_KEY,
    Backend,
    Tier,
    Upstream,
    create_garage_container,
    error,
    info,
    render_config,
    start_backend,
)

FIVE_MIB = 5 * 1024 * 1024


@dataclass
class Ctx:
    backend: Backend
    hot: Upstream
    cold: Upstream
    counter: int = 0

    @property
    def s3(self):
        return self.backend.client

    @property
    def presigner(self):
        # boto3 presigns with SigV2 by default, which Garage rejects.
        return boto3.client(
            "s3",
            endpoint_url=f"http://localhost:{BACKEND_PORT}",
            aws_access_key_id=GARAGE_DEFAULT_ACCESS_KEY,
            aws_secret_access_key=GARAGE_DEFAULT_SECRET_KEY,
            config=Config(signature_version="s3v4"),
        )

    @property
    def upstreams(self) -> list[Upstream]:
        return [self.hot, self.cold]

    def key(self, name: str) -> str:
        self.counter += 1
        return f"ops-{name}-{self.counter}"

    def put_hot(self, name: str, body: bytes | None = None) -> tuple[str, bytes]:
        key = self.key(name)
        body = body if body is not None else os.urandom(1024)
        self.s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body)
        self.wait_for_row(key, size=len(body), upstream=Tier.HOT)
        return key, body

    def put_cold_unindexed(self, name: str, size: int = 1024) -> tuple[str, bytes]:
        key = self.key(name)
        body = os.urandom(size)
        self.cold.client.put_object(Bucket=BUCKET_NAME, Key=key, Body=body)
        return key, body

    def put_cold_indexed(self, name: str, size: int = 1024) -> tuple[str, bytes]:
        key, body = self.put_cold_unindexed(name, size)
        self.s3.get_object(Bucket=BUCKET_NAME, Key=key)["Body"].read()
        self.wait_for_row(key, size=len(body), upstream=Tier.COLD)
        return key, body

    def row(self, key: str, bucket: str = BUCKET_NAME) -> tuple[str, int] | None:
        return self.backend.sqlite_connection.execute(
            "SELECT assigned_upstream, size FROM objects WHERE bucket = ? AND key = ?",
            (bucket, key),
        ).fetchone()

    def wait_for_row(
        self,
        key: str,
        *,
        size: int | None,
        upstream: Tier | None,
        bucket: str = BUCKET_NAME,
    ) -> None:
        """Waits for the async recording; `size=None, upstream=None` means no row."""
        expected = None if upstream is None else (upstream.value, size)
        eventually(
            lambda: self.row(key, bucket) == expected,
            lambda: f"DB row for {bucket}/{key}: expected {expected}, got {self.row(key, bucket)}",
        )

    def holders(self, key: str, bucket: str = BUCKET_NAME) -> list[str]:
        return [u.tier.value for u in self.upstreams if _exists(u, bucket, key)]

    def assert_only_on(self, key: str, tier: Tier, bucket: str = BUCKET_NAME) -> None:
        holders = self.holders(key, bucket)
        assert holders == [tier.value], (
            f"{key} expected on [{tier.value}], found {holders}"
        )

    def assert_gone(self, key: str) -> None:
        assert self.holders(key) == [], f"{key} still on {self.holders(key)}"
        self.wait_for_row(key, size=None, upstream=None)

    def get(self, key: str, **kwargs) -> bytes:
        return self.s3.get_object(Bucket=BUCKET_NAME, Key=key, **kwargs)["Body"].read()


def eventually(cond: Callable[[], bool], msg: Callable[[], str], timeout: float = 5):
    deadline = time.time() + timeout
    while not cond():
        if time.time() > deadline:
            raise AssertionError(msg())
        time.sleep(0.1)


def _exists(upstream: Upstream, bucket: str, key: str) -> bool:
    try:
        upstream.client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NoSuchBucket"):
            return False
        raise


def _bucket_exists(upstream: Upstream, bucket: str) -> bool:
    try:
        upstream.client.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


def _status(e: ClientError) -> int:
    return e.response["ResponseMetadata"]["HTTPStatusCode"]


def expect_status(status: int, call: Callable[[], object]) -> None:
    try:
        call()
    except ClientError as e:
        assert _status(e) == status, f"expected HTTP {status}, got {_status(e)}: {e}"
        return
    raise AssertionError(f"expected HTTP {status}, but the call succeeded")


# ---------------------------------------------------------------- buckets


def check_list_buckets(ctx: Ctx) -> None:
    names = [b["Name"] for b in ctx.s3.list_buckets()["Buckets"]]
    assert BUCKET_NAME in names, f"{BUCKET_NAME} not in {names}"


def check_head_bucket(ctx: Ctx) -> None:
    ctx.s3.head_bucket(Bucket=BUCKET_NAME)


def check_get_bucket_location(ctx: Ctx) -> None:
    ctx.s3.get_bucket_location(Bucket=BUCKET_NAME)


def check_create_bucket_rejected(ctx: Ctx) -> None:
    bucket = "ops-created-bucket"
    expect_status(501, lambda: ctx.s3.create_bucket(Bucket=bucket))
    created = [u.tier.value for u in ctx.upstreams if _bucket_exists(u, bucket)]
    assert not created, f"rejected bucket was created on {created}"


def check_delete_bucket_rejected(ctx: Ctx) -> None:
    bucket = "ops-deleted-bucket"
    for upstream in ctx.upstreams:
        upstream.client.create_bucket(Bucket=bucket)
    expect_status(501, lambda: ctx.s3.delete_bucket(Bucket=bucket))
    missing = [u.tier.value for u in ctx.upstreams if not _bucket_exists(u, bucket)]
    assert not missing, f"rejected bucket deletion removed it from {missing}"


def check_get_bucket_versioning(ctx: Ctx) -> None:
    status = ctx.s3.get_bucket_versioning(Bucket=BUCKET_NAME).get("Status")
    assert status in (None, "Suspended"), f"unexpected versioning status {status}"


def check_bucket_website(ctx: Ctx) -> None:
    ctx.s3.put_bucket_website(
        Bucket=BUCKET_NAME,
        WebsiteConfiguration={"IndexDocument": {"Suffix": "index.html"}},
    )
    got = ctx.s3.get_bucket_website(Bucket=BUCKET_NAME)
    assert got["IndexDocument"]["Suffix"] == "index.html", got
    # Garage keeps answering GetBucketWebsite after a delete, so only check the call.
    ctx.s3.delete_bucket_website(Bucket=BUCKET_NAME)


def check_bucket_cors(ctx: Ctx) -> None:
    rule = {"AllowedMethods": ["GET"], "AllowedOrigins": ["https://example.com"]}
    ctx.s3.put_bucket_cors(Bucket=BUCKET_NAME, CORSConfiguration={"CORSRules": [rule]})
    got = ctx.s3.get_bucket_cors(Bucket=BUCKET_NAME)["CORSRules"]
    assert got[0]["AllowedOrigins"] == ["https://example.com"], got
    ctx.s3.delete_bucket_cors(Bucket=BUCKET_NAME)
    expect_status(404, lambda: ctx.s3.get_bucket_cors(Bucket=BUCKET_NAME))


def check_bucket_lifecycle(ctx: Ctx) -> None:
    ctx.s3.put_bucket_lifecycle_configuration(
        Bucket=BUCKET_NAME,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "expire-tmp",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "tmp/"},
                    "Expiration": {"Days": 1},
                }
            ]
        },
    )
    got = ctx.s3.get_bucket_lifecycle_configuration(Bucket=BUCKET_NAME)["Rules"]
    assert got[0]["ID"] == "expire-tmp", got
    ctx.s3.delete_bucket_lifecycle(Bucket=BUCKET_NAME)
    expect_status(
        404, lambda: ctx.s3.get_bucket_lifecycle_configuration(Bucket=BUCKET_NAME)
    )


# ---------------------------------------------------------------- listing


def check_list_objects_v2(ctx: Ctx) -> None:
    hot, _ = ctx.put_hot("list2")
    cold, _ = ctx.put_cold_indexed("list2")
    keys, _ = _paginate_v2(ctx, Prefix="ops-list2-")
    assert keys == [hot, cold], f"expected [{hot}, {cold}], listed {keys}"


def check_list_objects_v1_rejected(ctx: Ctx) -> None:
    expect_status(501, lambda: ctx.s3.list_objects(Bucket=BUCKET_NAME))


def _paginate_v2(ctx: Ctx, **kwargs) -> tuple[list[str], list[str]]:
    keys, prefixes = [], []
    pages = ctx.s3.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET_NAME, **kwargs
    )
    for page in pages:
        keys.extend(o["Key"] for o in page.get("Contents", []))
        prefixes.extend(p["Prefix"] for p in page.get("CommonPrefixes", []))
    return keys, prefixes


def check_list_objects_v2_paginated(ctx: Ctx) -> None:
    prefix = ctx.key("list-pages") + "/"
    hot = [f"{prefix}hot-{i}" for i in range(3)]
    cold = [f"{prefix}cold-{i}" for i in range(3)]
    for key in hot:
        ctx.s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=b"h")
    for key in cold:
        ctx.cold.client.put_object(Bucket=BUCKET_NAME, Key=key, Body=b"c")
    keys, _ = _paginate_v2(ctx, Prefix=prefix, PaginationConfig={"PageSize": 1})
    assert keys == sorted(hot + cold), f"expected sorted keys, listed {keys}"


def check_list_objects_v2_interleaved(ctx: Ctx) -> None:
    prefix = ctx.key("list-interleaved") + "/"
    keys = [f"{prefix}{i:02}" for i in range(8)]
    for i, key in enumerate(keys):
        client = ctx.s3 if i % 2 == 0 else ctx.cold.client
        client.put_object(Bucket=BUCKET_NAME, Key=key, Body=b"x")
    for page_size in (1, 3, 1000):
        listed, _ = _paginate_v2(
            ctx, Prefix=prefix, PaginationConfig={"PageSize": page_size}
        )
        assert listed == keys, f"page size {page_size}: listed {listed}"


def check_list_objects_v2_sorted(ctx: Ctx) -> None:
    prefix = ctx.key("list-sorted") + "/"
    names = [
        "A",
        "Z",
        "_",
        "a",
        "a b",
        "a+b",
        "a%2Fb",
        "a-b",
        "a.b",
        "a~",
        "!",
        "~",
        "dir.x",
        "dir/x",
        "dir/y",
        "dir0",
        "é",
        "日本",
        "😀",
    ]
    keys = [prefix + name for name in names]
    for i, key in enumerate(keys):
        client = ctx.s3 if i % 3 else ctx.cold.client
        client.put_object(Bucket=BUCKET_NAME, Key=key, Body=b"x")

    expected = sorted(keys, key=str.encode)
    for page_size in (1, 2, 1000):
        listed, _ = _paginate_v2(
            ctx, Prefix=prefix, PaginationConfig={"PageSize": page_size}
        )
        assert listed == expected, f"page size {page_size}: listed {listed}"

        entries = []
        pages = ctx.s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET_NAME,
            Prefix=prefix,
            Delimiter="/",
            PaginationConfig={"PageSize": page_size},
        )
        for page in pages:
            entries += sorted(
                [o["Key"] for o in page.get("Contents", [])]
                + [p["Prefix"] for p in page.get("CommonPrefixes", [])],
                key=str.encode,
            )
        expected_entries = sorted(
            {k if "/" not in k[len(prefix) :] else prefix + "dir/" for k in keys},
            key=str.encode,
        )
        assert entries == expected_entries, f"page size {page_size}: listed {entries}"


def check_list_objects_v2_concurrent_listings(ctx: Ctx) -> None:
    # Both listings stop on hot's token for "k/2", only the wider one buffered "kz" from cold.
    base = ctx.key("list-concurrent")
    for key in (f"{base}k/1", f"{base}k/2"):
        ctx.s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=b"h")
    for key in (f"{base}k/1z", f"{base}kz"):
        ctx.cold.client.put_object(Bucket=BUCKET_NAME, Key=key, Body=b"c")

    def page(prefix: str, token: str | None) -> dict:
        kwargs = {"ContinuationToken": token} if token else {}
        return ctx.s3.list_objects_v2(
            Bucket=BUCKET_NAME, Prefix=prefix, MaxKeys=1, **kwargs
        )

    narrow, wide = f"{base}k/", f"{base}k"
    listed: dict[str, list[str]] = {narrow: [], wide: []}
    tokens: dict[str, str | None] = {narrow: None, wide: None}
    done: set[str] = set()
    while len(done) < 2:
        for prefix in (narrow, wide):
            if prefix in done:
                continue
            res = page(prefix, tokens[prefix])
            listed[prefix] += [o["Key"] for o in res.get("Contents", [])]
            tokens[prefix] = res.get("NextContinuationToken")
            if not res["IsTruncated"]:
                done.add(prefix)

    assert listed[narrow] == [f"{base}k/1", f"{base}k/1z", f"{base}k/2"], listed[narrow]
    assert listed[wide] == [f"{base}k/1", f"{base}k/1z", f"{base}k/2", f"{base}kz"], (
        listed[wide]
    )


def check_list_objects_v2_identical_concurrent_listings(ctx: Ctx) -> None:
    prefix = ctx.key("list-identical") + "/"
    keys = [f"{prefix}{i:02}" for i in range(6)]
    for i, key in enumerate(keys):
        client = ctx.s3 if i % 2 == 0 else ctx.cold.client
        client.put_object(Bucket=BUCKET_NAME, Key=key, Body=b"x")

    listed: list[list[str]] = [[], []]
    tokens: list[str | None] = [None, None]
    done: set[int] = set()
    while len(done) < 2:
        for i in (0, 1):
            if i in done:
                continue
            kwargs = {"ContinuationToken": tokens[i]} if tokens[i] else {}
            res = ctx.s3.list_objects_v2(
                Bucket=BUCKET_NAME, Prefix=prefix, MaxKeys=1, **kwargs
            )
            listed[i] += [o["Key"] for o in res.get("Contents", [])]
            tokens[i] = res.get("NextContinuationToken")
            if not res["IsTruncated"]:
                done.add(i)

    assert listed == [keys, keys], listed


def check_list_objects_v2_duplicates(ctx: Ctx) -> None:
    prefix = ctx.key("list-dup") + "/"
    for client in (ctx.hot.client, ctx.cold.client):
        client.put_object(Bucket=BUCKET_NAME, Key=f"{prefix}both", Body=b"x")
        client.put_object(Bucket=BUCKET_NAME, Key=f"{prefix}dir/inner", Body=b"x")
    # With page size 1 both upstreams run dry at the same key and hand out the same token
    for page_size in (1, 1000):
        config = {"PageSize": page_size}
        keys, _ = _paginate_v2(ctx, Prefix=prefix, PaginationConfig=config)
        assert keys == [f"{prefix}both", f"{prefix}dir/inner"], keys
        keys, prefixes = _paginate_v2(
            ctx, Prefix=prefix, Delimiter="/", PaginationConfig=config
        )
        assert keys == [f"{prefix}both"], keys
        assert prefixes == [f"{prefix}dir/"], prefixes


def check_list_objects_v2_delimiter(ctx: Ctx) -> None:
    prefix = ctx.key("list-delim") + "/"
    ctx.s3.put_object(Bucket=BUCKET_NAME, Key=f"{prefix}hot/a", Body=b"h")
    ctx.s3.put_object(Bucket=BUCKET_NAME, Key=f"{prefix}top", Body=b"h")
    ctx.cold.client.put_object(Bucket=BUCKET_NAME, Key=f"{prefix}cold/b", Body=b"c")
    keys, prefixes = _paginate_v2(ctx, Prefix=prefix, Delimiter="/")
    assert keys == [f"{prefix}top"], keys
    assert prefixes == [f"{prefix}cold/", f"{prefix}hot/"], prefixes


def check_list_objects_v2_only_cold(ctx: Ctx) -> None:
    prefix = ctx.key("list-only-cold") + "/"
    ctx.cold.client.put_object(Bucket=BUCKET_NAME, Key=f"{prefix}x", Body=b"c")
    res = ctx.s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    listed = [o["Key"] for o in res.get("Contents", [])]
    assert listed == [f"{prefix}x"], res
    assert not res["IsTruncated"], res


def check_list_objects_v2_unknown_token(ctx: Ctx) -> None:
    expect_status(
        400,
        lambda: ctx.s3.list_objects_v2(Bucket=BUCKET_NAME, ContinuationToken="bogus"),
    )


# ---------------------------------------------------------------- objects


def check_put_get_object(ctx: Ctx) -> None:
    key, body = ctx.put_hot("put")
    assert ctx.get(key) == body
    ctx.assert_only_on(key, Tier.HOT)


def check_put_overwrite_cold_object(ctx: Ctx) -> None:
    key, _ = ctx.put_cold_indexed("overwrite")
    body = os.urandom(2048)
    ctx.s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body)
    ctx.wait_for_row(key, size=len(body), upstream=Tier.COLD)
    assert ctx.get(key) == body
    ctx.assert_only_on(key, Tier.COLD)


def check_put_empty_object(ctx: Ctx) -> None:
    key, body = ctx.put_hot("empty", b"")
    assert ctx.get(key) == body


def check_put_aws_chunked(ctx: Ctx) -> None:
    key = ctx.key("chunked")
    body = os.urandom(70 * 1024)
    # A file-like body with a checksum makes botocore stream it as aws-chunked.
    with TemporaryDirectory() as d:
        path = Path(d) / "body"
        path.write_bytes(body)
        with path.open("rb") as f:
            ctx.s3.put_object(
                Bucket=BUCKET_NAME, Key=key, Body=f, ChecksumAlgorithm="CRC32"
            )
    ctx.wait_for_row(key, size=len(body), upstream=Tier.HOT)
    assert ctx.get(key) == body


def check_get_unindexed_cold_object(ctx: Ctx) -> None:
    key, body = ctx.put_cold_unindexed("get-unindexed")
    assert ctx.get(key) == body
    ctx.wait_for_row(key, size=len(body), upstream=Tier.COLD)


def check_get_range(ctx: Ctx) -> None:
    key, body = ctx.put_cold_indexed("range")
    assert ctx.get(key, Range="bytes=10-19") == body[10:20]


def check_get_if_none_match(ctx: Ctx) -> None:
    key, _ = ctx.put_hot("conditional")
    etag = ctx.s3.head_object(Bucket=BUCKET_NAME, Key=key)["ETag"]
    expect_status(304, lambda: ctx.get(key, IfNoneMatch=etag))


def check_get_missing_object(ctx: Ctx) -> None:
    expect_status(404, lambda: ctx.get(ctx.key("missing")))


def check_head_object(ctx: Ctx) -> None:
    key, body = ctx.put_cold_indexed("head")
    res = ctx.s3.head_object(Bucket=BUCKET_NAME, Key=key)
    assert res["ContentLength"] == len(body), res


def check_head_unindexed_cold_object(ctx: Ctx) -> None:
    key, body = ctx.put_cold_unindexed("head-unindexed")
    res = ctx.s3.head_object(Bucket=BUCKET_NAME, Key=key)
    assert res["ContentLength"] == len(body), res


def check_delete_object(ctx: Ctx) -> None:
    key, _ = ctx.put_cold_indexed("delete")
    ctx.s3.delete_object(Bucket=BUCKET_NAME, Key=key)
    ctx.assert_gone(key)


def check_delete_unindexed_cold_object(ctx: Ctx) -> None:
    key, _ = ctx.put_cold_unindexed("delete-unindexed")
    ctx.s3.delete_object(Bucket=BUCKET_NAME, Key=key)
    ctx.assert_gone(key)


def _delete_objects(client, keys: list[str], quiet: bool = False) -> dict[str, str]:
    """Deletes `keys` in one batch, returning `Deleted` or the error code per key."""
    res = client.delete_objects(
        Bucket=BUCKET_NAME,
        Delete={"Objects": [{"Key": k} for k in keys], "Quiet": quiet},
    )
    outcomes = {d["Key"]: "Deleted" for d in res.get("Deleted", [])}
    for e in res.get("Errors", []):
        assert e["Key"] not in outcomes, f"{e['Key']} both deleted and failed: {res}"
        outcomes[e["Key"]] = e["Code"]
    return outcomes


def check_delete_objects(ctx: Ctx) -> None:
    hot, _ = ctx.put_hot("batch-delete")
    cold, _ = ctx.put_cold_indexed("batch-delete")
    outcomes = _delete_objects(ctx.s3, [hot, cold])
    assert outcomes == {hot: "Deleted", cold: "Deleted"}, outcomes
    ctx.assert_gone(hot)
    ctx.assert_gone(cold)


def check_delete_objects_unindexed(ctx: Ctx) -> None:
    unindexed, _ = ctx.put_cold_unindexed("batch-delete-unindexed")
    hot, _ = ctx.put_hot("batch-delete-unindexed")
    outcomes = _delete_objects(ctx.s3, [unindexed, hot])
    assert outcomes == {unindexed: "Deleted", hot: "Deleted"}, outcomes
    ctx.assert_gone(unindexed)
    ctx.assert_gone(hot)


def check_delete_objects_on_both_upstreams(ctx: Ctx) -> None:
    key, body = ctx.put_hot("batch-delete-both")
    ctx.cold.client.put_object(Bucket=BUCKET_NAME, Key=key, Body=body)
    outcomes = _delete_objects(ctx.s3, [key])
    assert outcomes == {key: "Deleted"}, outcomes
    ctx.assert_gone(key)


def check_delete_objects_missing_key(ctx: Ctx) -> None:
    """Missing keys are reported like a single upstream reports them."""
    existing, _ = ctx.put_hot("batch-delete-present")
    missing = ctx.key("batch-delete-missing")
    expected = _delete_objects(ctx.hot.client, [missing])[missing]
    outcomes = _delete_objects(ctx.s3, [existing, missing])
    assert outcomes == {existing: "Deleted", missing: expected}, outcomes
    ctx.assert_gone(existing)


def check_delete_objects_quiet(ctx: Ctx) -> None:
    hot, _ = ctx.put_hot("batch-delete-quiet")
    cold, _ = ctx.put_cold_indexed("batch-delete-quiet")
    outcomes = _delete_objects(ctx.s3, [hot, cold], quiet=True)
    assert outcomes == {}, outcomes
    ctx.assert_gone(hot)
    ctx.assert_gone(cold)


def check_delete_objects_special_character_keys(ctx: Ctx) -> None:
    hot, _ = ctx.put_hot("batch & <delete> 'ä' \"x\"/y")
    cold, _ = ctx.put_cold_indexed("batch & <delete> 'é' \"x\"/y")
    outcomes = _delete_objects(ctx.s3, [hot, cold])
    assert outcomes == {hot: "Deleted", cold: "Deleted"}, outcomes
    ctx.assert_gone(hot)
    ctx.assert_gone(cold)


def check_delete_objects_duplicate_keys(ctx: Ctx) -> None:
    key, _ = ctx.put_cold_indexed("batch-delete-duplicate")
    res = ctx.s3.delete_objects(
        Bucket=BUCKET_NAME, Delete={"Objects": [{"Key": key}, {"Key": key}]}
    )
    assert not res.get("Errors"), res["Errors"]
    assert [d["Key"] for d in res["Deleted"]] == [key], res["Deleted"]
    ctx.assert_gone(key)


def check_delete_objects_full_batch(ctx: Ctx) -> None:
    hot = [ctx.put_hot("batch-delete-full")[0] for _ in range(10)]
    cold = [ctx.put_cold_indexed("batch-delete-full")[0] for _ in range(10)]
    unindexed = [ctx.put_cold_unindexed("batch-delete-full")[0] for _ in range(10)]
    present = hot + cold + unindexed
    missing = [ctx.key("batch-delete-full-missing") for _ in range(1000 - len(present))]
    expected = _delete_objects(ctx.hot.client, missing[:1])[missing[0]]

    outcomes = _delete_objects(ctx.s3, present + missing)

    assert len(outcomes) == 1000, f"{len(outcomes)} outcomes for 1000 keys"
    wrong = {k: v for k, v in outcomes.items() if v != "Deleted" and k in present}
    assert not wrong, f"present keys not deleted: {wrong}"
    wrong = {k: v for k, v in outcomes.items() if v != expected and k in missing}
    assert not wrong, f"missing keys not reported as {expected}: {wrong}"
    for key in present:
        ctx.assert_gone(key)


def check_copy_object_same_upstream(ctx: Ctx) -> None:
    src, body = ctx.put_hot("copy-src")
    dst = ctx.key("copy-dst")
    ctx.s3.copy_object(
        Bucket=BUCKET_NAME, Key=dst, CopySource={"Bucket": BUCKET_NAME, "Key": src}
    )
    ctx.wait_for_row(dst, size=len(body), upstream=Tier.HOT)
    assert ctx.get(dst) == body


def check_copy_object_from_cold(ctx: Ctx) -> None:
    src, body = ctx.put_cold_indexed("copy-cold-src")
    dst = ctx.key("copy-cold-dst")
    ctx.s3.copy_object(
        Bucket=BUCKET_NAME, Key=dst, CopySource={"Bucket": BUCKET_NAME, "Key": src}
    )
    assert ctx.get(dst) == body
    row = ctx.row(dst)
    assert row is not None and row[1] == len(body), f"DB row for {dst}: {row}"


def check_copy_object_onto_other_upstream(ctx: Ctx) -> None:
    src, body = ctx.put_cold_indexed("copy-over-src")
    dst, _ = ctx.put_hot("copy-over-dst")
    ctx.s3.copy_object(
        Bucket=BUCKET_NAME, Key=dst, CopySource={"Bucket": BUCKET_NAME, "Key": src}
    )
    ctx.wait_for_row(dst, size=len(body), upstream=Tier.COLD)
    assert ctx.get(dst) == body
    eventually(
        lambda: ctx.holders(dst) == [Tier.COLD.value],
        lambda: f"{dst} expected only on cold, found {ctx.holders(dst)}",
    )


def check_copy_object_special_character_source(ctx: Ctx) -> None:
    src = ctx.key("copy späcial/+&=?#%")
    body = os.urandom(256)
    ctx.cold.client.put_object(Bucket=BUCKET_NAME, Key=src, Body=body)
    dst = ctx.key("copy-special-dst")
    ctx.s3.copy_object(
        Bucket=BUCKET_NAME, Key=dst, CopySource={"Bucket": BUCKET_NAME, "Key": src}
    )
    ctx.wait_for_row(dst, size=len(body), upstream=Tier.COLD)
    assert ctx.get(dst) == body


def check_presigned_get(ctx: Ctx) -> None:
    key, body = ctx.put_hot("presigned")
    url = ctx.presigner.generate_presigned_url(
        "get_object", Params={"Bucket": BUCKET_NAME, "Key": key}, ExpiresIn=60
    )
    res = requests.get(url, timeout=10)
    assert res.status_code == 200, f"HTTP {res.status_code}: {res.text}"
    assert res.content == body


def check_presigned_put(ctx: Ctx) -> None:
    key = ctx.key("presigned-put")
    body = os.urandom(512)
    url = ctx.presigner.generate_presigned_url(
        "put_object", Params={"Bucket": BUCKET_NAME, "Key": key}, ExpiresIn=60
    )
    res = requests.put(url, data=body, timeout=10)
    assert res.status_code == 200, f"HTTP {res.status_code}: {res.text}"
    ctx.wait_for_row(key, size=len(body), upstream=Tier.HOT)


def check_post_object(ctx: Ctx) -> None:
    key = ctx.key("post-object")
    body = os.urandom(512)
    post = ctx.presigner.generate_presigned_post(BUCKET_NAME, key, ExpiresIn=60)
    res = requests.post(
        post["url"], data=post["fields"], files={"file": body}, timeout=10
    )
    assert res.status_code in (200, 204), f"HTTP {res.status_code}: {res.text}"
    assert ctx.get(key) == body
    ctx.wait_for_row(key, size=len(body), upstream=Tier.HOT)


def check_post_object_filename_placeholder(ctx: Ctx) -> None:
    prefix = ctx.key("post-filename") + "/"
    body = os.urandom(512)
    post = ctx.presigner.generate_presigned_post(
        BUCKET_NAME,
        prefix + "${filename}",
        Conditions=[["starts-with", "$key", prefix]],
        ExpiresIn=60,
    )
    res = requests.post(
        post["url"],
        data=post["fields"],
        files={"file": ("up load.bin", body)},
        timeout=10,
    )
    assert res.status_code in (200, 204), f"HTTP {res.status_code}: {res.text}"
    key = prefix + "up load.bin"
    assert ctx.get(key) == body
    ctx.wait_for_row(key, size=len(body), upstream=Tier.HOT)


def check_post_object_overwrite_cold_object(ctx: Ctx) -> None:
    key, _ = ctx.put_cold_indexed("post-overwrite")
    body = os.urandom(512)
    post = ctx.presigner.generate_presigned_post(BUCKET_NAME, key, ExpiresIn=60)
    res = requests.post(
        post["url"], data=post["fields"], files={"file": body}, timeout=10
    )
    assert res.status_code in (200, 204), f"HTTP {res.status_code}: {res.text}"
    ctx.wait_for_row(key, size=len(body), upstream=Tier.COLD)
    assert ctx.get(key) == body
    ctx.assert_only_on(key, Tier.COLD)


def check_sse_c_rejected(ctx: Ctx) -> None:
    key = ctx.key("sse-c")
    sse = {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": os.urandom(32)}
    expect_status(
        501, lambda: ctx.s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=b"x", **sse)
    )
    assert ctx.holders(key) == [], f"rejected {key} was stored on {ctx.holders(key)}"
    assert ctx.row(key) is None, ctx.row(key)


def check_user_metadata(ctx: Ctx) -> None:
    key = ctx.key("metadata")
    ctx.s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=b"meta",
        Metadata={"origin": "e2e"},
        ContentType="text/x-parkhaus",
    )
    res = ctx.s3.head_object(Bucket=BUCKET_NAME, Key=key)
    assert res["Metadata"] == {"origin": "e2e"}, res["Metadata"]
    assert res["ContentType"] == "text/x-parkhaus", res["ContentType"]


def check_special_character_key(ctx: Ctx) -> None:
    key = ctx.key("späcial chars/+&=?#%")
    body = b"special"
    ctx.s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body)
    assert ctx.get(key) == body
    ctx.wait_for_row(key, size=len(body), upstream=Tier.HOT)


# ---------------------------------------------------------------- multipart


def check_list_multipart_uploads(ctx: Ctx) -> None:
    key = ctx.key("list-uploads")
    upload_id = ctx.s3.create_multipart_upload(Bucket=BUCKET_NAME, Key=key)["UploadId"]
    try:
        uploads = ctx.s3.list_multipart_uploads(Bucket=BUCKET_NAME).get("Uploads", [])
        assert any(u["UploadId"] == upload_id for u in uploads), uploads
    finally:
        ctx.s3.abort_multipart_upload(Bucket=BUCKET_NAME, Key=key, UploadId=upload_id)
    ctx.wait_for_row(key, size=None, upstream=None)


def check_multipart_overwrite_cold_object(ctx: Ctx) -> None:
    key, _ = ctx.put_cold_indexed("mpu-cold")
    upload_id = ctx.s3.create_multipart_upload(Bucket=BUCKET_NAME, Key=key)["UploadId"]
    body = os.urandom(4096)
    etag = ctx.s3.upload_part(
        Bucket=BUCKET_NAME, Key=key, UploadId=upload_id, PartNumber=1, Body=body
    )["ETag"]
    ctx.s3.complete_multipart_upload(
        Bucket=BUCKET_NAME,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"ETag": etag, "PartNumber": 1}]},
    )
    ctx.wait_for_row(key, size=len(body), upstream=Tier.COLD)
    assert ctx.get(key) == body
    ctx.assert_only_on(key, Tier.COLD)


def check_abort_keeps_existing_object(ctx: Ctx) -> None:
    key, body = ctx.put_hot("abort-existing")
    upload_id = ctx.s3.create_multipart_upload(Bucket=BUCKET_NAME, Key=key)["UploadId"]
    ctx.s3.abort_multipart_upload(Bucket=BUCKET_NAME, Key=key, UploadId=upload_id)
    time.sleep(0.5)
    assert ctx.get(key) == body
    ctx.wait_for_row(key, size=len(body), upstream=Tier.HOT)


def _upload_part_copy(ctx: Ctx, src: str, src_body: bytes) -> None:
    dst = ctx.key("part-copy-dst")
    upload_id = ctx.s3.create_multipart_upload(Bucket=BUCKET_NAME, Key=dst)["UploadId"]
    first = os.urandom(FIVE_MIB)
    parts = [
        ctx.s3.upload_part(
            Bucket=BUCKET_NAME, Key=dst, UploadId=upload_id, PartNumber=1, Body=first
        )["ETag"],
        ctx.s3.upload_part_copy(
            Bucket=BUCKET_NAME,
            Key=dst,
            UploadId=upload_id,
            PartNumber=2,
            CopySource={"Bucket": BUCKET_NAME, "Key": src},
        )["CopyPartResult"]["ETag"],
    ]
    ctx.s3.complete_multipart_upload(
        Bucket=BUCKET_NAME,
        Key=dst,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [{"ETag": e, "PartNumber": i} for i, e in enumerate(parts, 1)]
        },
    )
    expected = first + src_body
    ctx.wait_for_row(dst, size=len(expected), upstream=Tier.HOT)
    got = ctx.get(dst)
    assert hashlib.sha256(got).digest() == hashlib.sha256(expected).digest()


def check_upload_part_copy_same_upstream(ctx: Ctx) -> None:
    # Garage rejects copy sources below the minimum part size, even for the last part.
    src, body = ctx.put_hot("part-copy-src", os.urandom(FIVE_MIB))
    _upload_part_copy(ctx, src, body)


def check_upload_part_copy_from_cold_rejected(ctx: Ctx) -> None:
    src, _ = ctx.put_cold_indexed("part-copy-cold-src", FIVE_MIB)
    dst = ctx.key("part-copy-cold-dst")
    upload_id = ctx.s3.create_multipart_upload(Bucket=BUCKET_NAME, Key=dst)["UploadId"]
    try:
        expect_status(
            501,
            lambda: ctx.s3.upload_part_copy(
                Bucket=BUCKET_NAME,
                Key=dst,
                UploadId=upload_id,
                PartNumber=1,
                CopySource={"Bucket": BUCKET_NAME, "Key": src},
            ),
        )
    finally:
        ctx.s3.abort_multipart_upload(Bucket=BUCKET_NAME, Key=dst, UploadId=upload_id)


# ---------------------------------------------------------------- unsupported


def _check_not_implemented(
    call: Callable[[Ctx, str], object],
) -> Callable[[Ctx], None]:
    def check(ctx: Ctx) -> None:
        key, body = ctx.put_hot("unsupported")
        expect_status(501, lambda: call(ctx, key))
        time.sleep(0.5)
        assert ctx.row(key) == (Tier.HOT.value, len(body)), ctx.row(key)
        assert ctx.get(key) == body

    return check


CHECKS: list[tuple[str, Callable[[Ctx], None]]] = [
    ("ListBuckets", check_list_buckets),
    ("HeadBucket", check_head_bucket),
    ("GetBucketLocation", check_get_bucket_location),
    ("CreateBucket (rejected with 501)", check_create_bucket_rejected),
    ("DeleteBucket (rejected with 501)", check_delete_bucket_rejected),
    ("GetBucketVersioning", check_get_bucket_versioning),
    ("Put/Get/DeleteBucketWebsite", check_bucket_website),
    ("Put/Get/DeleteBucketCors", check_bucket_cors),
    ("Put/Get/DeleteBucketLifecycle", check_bucket_lifecycle),
    ("ListObjectsV2 (hot + cold)", check_list_objects_v2),
    ("ListObjectsV2 paginated across upstreams", check_list_objects_v2_paginated),
    ("ListObjectsV2 interleaved keys, sorted", check_list_objects_v2_interleaved),
    ("ListObjectsV2 sorted by UTF-8 bytes", check_list_objects_v2_sorted),
    ("ListObjectsV2 deduplicates keys and prefixes", check_list_objects_v2_duplicates),
    ("ListObjectsV2 concurrent listings", check_list_objects_v2_concurrent_listings),
    (
        "ListObjectsV2 identical concurrent listings",
        check_list_objects_v2_identical_concurrent_listings,
    ),
    ("ListObjectsV2 delimiter across upstreams", check_list_objects_v2_delimiter),
    ("ListObjectsV2 key only on cold", check_list_objects_v2_only_cold),
    ("ListObjectsV2 unknown token (400)", check_list_objects_v2_unknown_token),
    ("ListObjects v1 (rejected with 501)", check_list_objects_v1_rejected),
    ("PutObject + GetObject", check_put_get_object),
    ("PutObject overwrite of cold object", check_put_overwrite_cold_object),
    ("PutObject empty body", check_put_empty_object),
    ("PutObject aws-chunked body", check_put_aws_chunked),
    ("PutObject user metadata", check_user_metadata),
    ("PutObject special-character key", check_special_character_key),
    ("PutObject SSE-C (rejected with 501)", check_sse_c_rejected),
    ("GetObject unindexed cold object", check_get_unindexed_cold_object),
    ("GetObject Range", check_get_range),
    ("GetObject If-None-Match", check_get_if_none_match),
    ("GetObject missing key", check_get_missing_object),
    ("HeadObject", check_head_object),
    ("HeadObject unindexed cold object", check_head_unindexed_cold_object),
    ("DeleteObject", check_delete_object),
    ("DeleteObject unindexed cold object", check_delete_unindexed_cold_object),
    ("DeleteObjects (hot + cold)", check_delete_objects),
    ("DeleteObjects unindexed cold object", check_delete_objects_unindexed),
    ("DeleteObjects key on both upstreams", check_delete_objects_on_both_upstreams),
    ("DeleteObjects missing key", check_delete_objects_missing_key),
    ("DeleteObjects quiet", check_delete_objects_quiet),
    ("DeleteObjects special-character keys", check_delete_objects_special_character_keys),
    ("DeleteObjects duplicate keys", check_delete_objects_duplicate_keys),
    ("DeleteObjects 1000 keys", check_delete_objects_full_batch),
    ("CopyObject same upstream", check_copy_object_same_upstream),
    ("CopyObject from cold source", check_copy_object_from_cold),
    ("CopyObject onto object on other upstream", check_copy_object_onto_other_upstream),
    ("CopyObject special-character source", check_copy_object_special_character_source),
    ("Presigned GET", check_presigned_get),
    ("Presigned PUT", check_presigned_put),
    ("PostObject", check_post_object),
    ("PostObject ${filename} key", check_post_object_filename_placeholder),
    ("PostObject overwrite of cold object", check_post_object_overwrite_cold_object),
    ("ListMultipartUploads", check_list_multipart_uploads),
    ("Multipart overwrite of cold object", check_multipart_overwrite_cold_object),
    ("AbortMultipartUpload keeps existing object", check_abort_keeps_existing_object),
    ("UploadPartCopy same upstream", check_upload_part_copy_same_upstream),
    (
        "UploadPartCopy from cold source (rejected with 501)",
        check_upload_part_copy_from_cold_rejected,
    ),
    (
        "PutObjectTagging (501, DB untouched)",
        _check_not_implemented(
            lambda ctx, key: ctx.s3.put_object_tagging(
                Bucket=BUCKET_NAME,
                Key=key,
                Tagging={"TagSet": [{"Key": "a", "Value": "b"}]},
            )
        ),
    ),
    (
        "PutObjectAcl (501, DB untouched)",
        _check_not_implemented(
            lambda ctx, key: ctx.s3.put_object_acl(
                Bucket=BUCKET_NAME, Key=key, ACL="private"
            )
        ),
    ),
    (
        "RestoreObject (501, DB untouched)",
        _check_not_implemented(
            lambda ctx, key: ctx.s3.restore_object(
                Bucket=BUCKET_NAME, Key=key, RestoreRequest={"Days": 1}
            )
        ),
    ),
]


def _run_checks(ctx: Ctx) -> list[tuple[str, str | None]]:
    results = []
    for name, check in CHECKS:
        info(f"Checking {name}", level=2)
        try:
            check(ctx)
            results.append((name, None))
        except Exception as e:
            error(f"{name}: {e}", level=4)
            if not isinstance(e, AssertionError | ClientError):
                traceback.print_exc()
            results.append((name, str(e).strip().splitlines()[0]))
    return results


def main() -> None:
    info("Starting S3 operations e2e test...")

    with (
        create_garage_container("hot") as hot_container,
        create_garage_container("cold") as cold_container,
        TemporaryDirectory(prefix="operations-e2e-") as temp_dir_str,
    ):
        buckets = [BUCKET_NAME]
        hot = Upstream.create(Tier.HOT, hot_container, buckets)
        cold = Upstream.create(Tier.COLD, cold_container, buckets)

        temp_dir = Path(temp_dir_str)
        config_path = temp_dir / "config.toml"
        # Never migrate, objects must stay where the checks put them.
        config_path.write_text(
            render_config(temp_dir, [hot, cold], [(Tier.COLD, "false")])
        )

        with start_backend(temp_dir, config_path) as backend:
            assert backend.client.meta.endpoint_url.endswith(str(BACKEND_PORT))
            results = _run_checks(Ctx(backend, hot, cold))

            table = Table(title="S3 operations through parkhaus")
            table.add_column("Operation")
            table.add_column("Result")
            table.add_column("Failure")
            for name, failure in results:
                table.add_row(
                    name,
                    "[green]PASS[/]" if failure is None else "[red]FAIL[/]",
                    failure or "",
                )
            print(table)

            failed = [name for name, failure in results if failure is not None]
            if failed:
                raise AssertionError(f"{len(failed)}/{len(results)} checks failed")

    info("S3 operations e2e test completed successfully!")
