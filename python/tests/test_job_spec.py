"""JobSpec Arrow IPC round-trip tests."""

from __future__ import annotations

from datetime import datetime, timezone

from mantle_analytics.job_spec import (
    JobSpec,
    ServiceRef,
    decode_job_spec,
    encode_job_spec,
)


def test_decode_job_spec_matches_mantle_arrow_schema() -> None:
    submitted = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    job = JobSpec(
        job_id="550e8400-e29b-41d4-a716-446655440000",
        process_id="ndvi",
        params={"red": 0.1, "nir": 0.8},
        submitted_at=submitted,
        service_refs=[
            ServiceRef(
                id="550e8400-e29b-41d4-a716-446655440001",
                name="sentinel-2",
                format="cog",
                storage_uri="s3://mantle-data/services/s2.tif",
            )
        ],
    )

    payload = encode_job_spec(job)
    decoded = decode_job_spec(payload)

    assert decoded.job_id == job.job_id
    assert decoded.process_id == "ndvi"
    assert decoded.params == {"red": 0.1, "nir": 0.8}
    assert len(decoded.service_refs) == 1
    assert decoded.service_refs[0].storage_uri.endswith("s2.tif")
    assert decoded.submitted_at == submitted
