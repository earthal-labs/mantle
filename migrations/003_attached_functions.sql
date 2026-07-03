-- Virtual services: on-the-fly attached functions and batch output datasets.
-- Attached services reference a parent dataset (no duplicate storage_uri).
-- Output services reference a newly created output dataset after a pRPM job.

CREATE TABLE IF NOT EXISTS virtual_services (
    id              UUID PRIMARY KEY,
    slug            TEXT NOT NULL UNIQUE,
    service_kind    TEXT NOT NULL CHECK (service_kind IN ('attached', 'output')),
    dataset_id      UUID NOT NULL REFERENCES datasets(id),
    parent_dataset_id UUID REFERENCES datasets(id),
    function_id     TEXT NOT NULL,
    params_defaults JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS virtual_services_slug_idx ON virtual_services (slug);
CREATE INDEX IF NOT EXISTS virtual_services_dataset_id_idx ON virtual_services (dataset_id);
CREATE INDEX IF NOT EXISTS virtual_services_parent_dataset_id_idx ON virtual_services (parent_dataset_id);
