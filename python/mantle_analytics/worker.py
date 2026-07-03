"""Analytics worker — Redis Streams consumer and Ray executor."""

from __future__ import annotations

import base64
import os
import signal
import socket
import sys
from typing import Any

import ray
import redis

from mantle_analytics.config import load_settings
from mantle_analytics.job_spec import decode_job_spec
from mantle_analytics.status import JobStatus, write_status
from mantle_analytics.tasks import dispatch_process

CONSUMER_GROUP = "mantle-analytics"
_shutdown = False


def _handle_signal(signum: int, _frame: object) -> None:
    global _shutdown
    print(f"mantle-analytics received signal {signum}, shutting down", flush=True)
    _shutdown = True


def _ensure_consumer_group(client: redis.Redis, stream_key: str) -> None:
    try:
        client.xgroup_create(stream_key, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.ResponseError as err:
        if "BUSYGROUP" not in str(err):
            raise


def _decode_payload(fields: dict[Any, Any]) -> bytes:
    raw = fields.get(b"payload") or fields.get("payload")
    if raw is None:
        raise ValueError("stream message missing payload field")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return base64.b64decode(raw)


def _process_message(
    client: redis.Redis,
    stream_key: str,
    message_id: str,
    fields: dict[Any, Any],
    *,
    bucket: str,
) -> None:
    payload = _decode_payload(fields)
    job = decode_job_spec(payload)

    write_status(
        client,
        job.job_id,
        JobStatus(state="running", progress=0.1),
    )

    try:
        ref = dispatch_process(job, bucket)
        write_status(
            client,
            job.job_id,
            JobStatus(state="running", progress=0.5),
        )
        result = ray.get(ref)
        write_status(
            client,
            job.job_id,
            JobStatus(
                state="succeeded",
                progress=1.0,
                result_url=str(result.get("result_url")),
            ),
        )
    except Exception as exc:  # noqa: BLE001 - job boundary must capture all failures
        write_status(
            client,
            job.job_id,
            JobStatus(state="failed", progress=1.0, error=str(exc)),
        )
        print(f"job {job.job_id} failed: {exc}", file=sys.stderr, flush=True)

    client.xack(stream_key, CONSUMER_GROUP, message_id)


def run_worker(
    *,
    config_path: str | None = None,
    block_ms: int = 5000,
) -> None:
    settings = load_settings(config_path)
    consumer_name = os.environ.get(
        "MANTLE_CONSUMER_NAME",
        f"mantle-analytics-{socket.gethostname()}",
    )

    client = redis.Redis.from_url(settings.redis_url, decode_responses=False)
    _ensure_consumer_group(client, settings.stream_key)

    if not ray.is_initialized():
        ray.init(address=settings.ray_address, ignore_reinit_error=True)

    print(
        f"mantle-analytics worker started stream={settings.stream_key} "
        f"ray={settings.ray_address} consumer={consumer_name}",
        flush=True,
    )

    while not _shutdown:
        messages = client.xreadgroup(
            CONSUMER_GROUP,
            consumer_name,
            {settings.stream_key: ">"},
            count=1,
            block=block_ms,
        )
        if not messages:
            continue

        for _stream, entries in messages:
            for message_id, fields in entries:
                msg_id = (
                    message_id.decode("utf-8")
                    if isinstance(message_id, bytes)
                    else message_id
                )
                _process_message(
                    client,
                    settings.stream_key,
                    msg_id,
                    fields,
                    bucket=settings.storage_bucket,
                )

    if ray.is_initialized():
        ray.shutdown()
    print("mantle-analytics worker stopped", flush=True)


def main() -> None:
    config_path = os.environ.get("MANTLE_CONFIG", "config.toml")
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        run_worker(config_path=config_path)
    except KeyboardInterrupt:
        pass

    sys.exit(0)


if __name__ == "__main__":
    main()
