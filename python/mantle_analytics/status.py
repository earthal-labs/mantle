"""Job status keys in Redis — ``mantle:job:{id}`` contract."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

JOB_KEY_PREFIX = "mantle:job:"
JOB_STATUS_TTL_SECONDS = 604_800

JobState = Literal["pending", "running", "succeeded", "failed"]


def job_key(job_id: str) -> str:
    return f"{JOB_KEY_PREFIX}{job_id}"


@dataclass
class JobStatus:
    state: JobState
    progress: float
    result_url: str | None = None
    error: str | None = None

    def to_json(self) -> str:
        payload: dict[str, Any] = {
            "state": self.state,
            "progress": self.progress,
        }
        if self.result_url is not None:
            payload["result_url"] = self.result_url
        if self.error is not None:
            payload["error"] = self.error
        return json.dumps(payload)

    @classmethod
    def from_json(cls, raw: str) -> JobStatus:
        data = json.loads(raw)
        return cls(
            state=data["state"],
            progress=float(data.get("progress", 0.0)),
            result_url=data.get("result_url"),
            error=data.get("error"),
        )


def write_status(
    redis_client: Any,
    job_id: str,
    status: JobStatus,
    *,
    ttl_seconds: int = JOB_STATUS_TTL_SECONDS,
) -> None:
    redis_client.set(job_key(job_id), status.to_json(), ex=ttl_seconds)
