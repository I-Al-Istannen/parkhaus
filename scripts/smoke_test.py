#!/usr/bin/env python3
# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "requests>=2.32.5",
# ]
# ///

import argparse
import json
import logging
import os
import random
import subprocess
import time
import uuid
from dataclasses import dataclass

import requests


LOGGER = logging.getLogger("smoke-test")


@dataclass
class Endpoints:
    hot: str
    cold: str
    proxy: str


def random_bytes(size: int) -> bytes:
    return os.urandom(size)


def endpoint_health(endpoint: str) -> int:
    response = requests.get(endpoint.rstrip("/") + "/", timeout=5)
    return response.status_code


def ensure_bucket(endpoint: str, bucket: str, label: str) -> None:
    LOGGER.info("Creating bucket on %s: %s", label, bucket)
    url = f"{endpoint.rstrip('/')}/{bucket}"
    response = requests.put(url, timeout=10)
    if response.status_code not in (200, 201):
        raise RuntimeError(f"create bucket failed on {label}: {response.status_code} {response.text}")


def put_object(endpoint: str, bucket: str, key: str, payload: bytes, label: str) -> None:
    LOGGER.info("PUT via %s: s3://%s/%s (%d bytes)", label, bucket, key, len(payload))
    url = f"{endpoint.rstrip('/')}/{bucket}/{key}"
    response = requests.put(url, data=payload, timeout=10)
    if response.status_code not in (200, 201):
        raise RuntimeError(f"put object failed on {label}: {response.status_code} {response.text}")


def get_object_header(endpoint: str, bucket: str, key: str) -> tuple[int, str | None, bytes]:
    url = f"{endpoint.rstrip('/')}/{bucket}/{key}"
    response = requests.get(url, timeout=10)
    upstream = response.headers.get("x-tiering-upstream")
    return response.status_code, upstream, response.content


def delete_object_http(endpoint: str, bucket: str, key: str) -> tuple[int, str | None]:
    url = f"{endpoint.rstrip('/')}/{bucket}/{key}"
    response = requests.delete(url, timeout=10)
    return response.status_code, response.headers.get("x-tiering-upstream")


def put_object_http(endpoint: str, bucket: str, key: str, payload: bytes) -> tuple[int, str | None]:
    url = f"{endpoint.rstrip('/')}/{bucket}/{key}"
    response = requests.put(url, data=payload, timeout=10)
    return response.status_code, response.headers.get("x-tiering-upstream")


def run_import(config_path: str) -> None:
    LOGGER.info("Running import command against %s", config_path)
    subprocess.run(
        ["cargo", "run", "--", "--config", config_path, "import"],
        check=True,
        text=True,
    )


def assert_upstream(name: str, got: str | None, expected: str) -> None:
    if got != expected:
        raise RuntimeError(f"{name}: expected upstream header {expected!r}, got {got!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Tiering proxy smoke test")
    parser.add_argument("--hot", default="http://127.0.0.1:9090")
    parser.add_argument("--cold", default="http://127.0.0.1:9091")
    parser.add_argument("--proxy", default="http://127.0.0.1:8080")
    parser.add_argument("--config", default="config.example.toml")
    parser.add_argument("--run-import", action="store_true")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    random.seed(args.seed)
    endpoints = Endpoints(hot=args.hot, cold=args.cold, proxy=args.proxy)
    bucket = f"tiering-smoke-{uuid.uuid4().hex[:10]}"

    LOGGER.info("Using bucket: %s", bucket)
    for name, endpoint in (("hot", endpoints.hot), ("cold", endpoints.cold), ("proxy", endpoints.proxy)):
        status = endpoint_health(endpoint)
        LOGGER.info("Endpoint %s reachable: status=%s url=%s", name, status, endpoint)

    ensure_bucket(endpoints.hot, bucket, "hot")
    ensure_bucket(endpoints.cold, bucket, "cold")

    hot_key = "only-hot.bin"
    cold_key = "only-cold.bin"
    shared_key = "shared.bin"

    hot_payload = random_bytes(128)
    cold_payload = random_bytes(160)
    shared_hot = random_bytes(64)
    shared_cold = random_bytes(96)

    put_object(endpoints.hot, bucket, hot_key, hot_payload, "hot")
    put_object(endpoints.cold, bucket, cold_key, cold_payload, "cold")
    put_object(endpoints.hot, bucket, shared_key, shared_hot, "hot")
    time.sleep(1.2)
    put_object(endpoints.cold, bucket, shared_key, shared_cold, "cold")

    if args.run_import:
        run_import(args.config)

    status, upstream, body = get_object_header(endpoints.proxy, bucket, hot_key)
    LOGGER.info("GET hot-key -> status=%s upstream=%s len=%d", status, upstream, len(body))
    assert status == 200
    assert_upstream("GET hot-key", upstream, "hot")

    status, upstream, body = get_object_header(endpoints.proxy, bucket, cold_key)
    LOGGER.info("GET cold-key -> status=%s upstream=%s len=%d", status, upstream, len(body))
    assert status == 200
    assert_upstream("GET cold-key", upstream, "cold")

    status, upstream, body = get_object_header(endpoints.proxy, bucket, shared_key)
    LOGGER.info("GET shared-key -> status=%s upstream=%s len=%d", status, upstream, len(body))
    assert status == 200
    assert_upstream("GET shared-key", upstream, "cold")

    via_proxy_key = "via-proxy.bin"
    via_proxy_payload = random_bytes(200)
    status, upstream = put_object_http(endpoints.proxy, bucket, via_proxy_key, via_proxy_payload)
    LOGGER.info("PUT via-proxy-key -> status=%s upstream=%s", status, upstream)
    assert status in (200, 201)
    assert_upstream("PUT via-proxy-key", upstream, "hot")

    status, upstream, body = get_object_header(endpoints.proxy, bucket, via_proxy_key)
    LOGGER.info("GET via-proxy-key -> status=%s upstream=%s len=%d", status, upstream, len(body))
    assert status == 200
    assert_upstream("GET via-proxy-key", upstream, "hot")

    status, upstream = delete_object_http(endpoints.proxy, bucket, via_proxy_key)
    LOGGER.info("DELETE via-proxy-key -> status=%s upstream=%s", status, upstream)
    assert status in (200, 204)
    assert_upstream("DELETE via-proxy-key", upstream, "hot")

    status, upstream, body = get_object_header(endpoints.proxy, bucket, via_proxy_key)
    LOGGER.info("GET deleted via-proxy-key -> status=%s upstream=%s", status, upstream)
    assert status == 404
    assert_upstream("GET deleted via-proxy-key", upstream, "hot")

    LOGGER.info("Smoke test succeeded")
    print(json.dumps({"bucket": bucket, "status": "ok"}))


if __name__ == "__main__":
    main()
