"""Job status Redis key format tests."""

from __future__ import annotations

import json

from mantle_analytics.status import JOB_KEY_PREFIX, JobStatus, job_key


def test_job_key_matches_agents_md_contract() -> None:
    job_id = "550e8400-e29b-41d4-a716-446655440000"
    assert job_key(job_id) == f"{JOB_KEY_PREFIX}{job_id}"
    assert job_key(job_id).startswith("mantle:job:")


def test_job_status_json_round_trip() -> None:
    status = JobStatus(
        state="running",
        progress=0.42,
        result_url=None,
        error=None,
    )
    restored = JobStatus.from_json(status.to_json())
    assert restored.state == "running"
    assert restored.progress == 0.42


def test_job_status_includes_result_and_error_when_set() -> None:
    status = JobStatus(
        state="failed",
        progress=1.0,
        result_url="s3://mantle-data/jobs/x/out.json",
        error="boom",
    )
    payload = json.loads(status.to_json())
    assert payload["state"] == "failed"
    assert payload["result_url"].startswith("s3://")
    assert payload["error"] == "boom"
