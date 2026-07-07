-- Virtual services: on-the-fly attached functions and batch output services.
-- Attached services reference a parent service (no duplicate storage_uri).
-- Output services reference a newly created output service after a pRPM job.

CREATE TABLE IF NOT EXISTS virtual_services (
    id              UUID PRIMARY KEY,
    slug            TEXT NOT NULL UNIQUE,
    service_kind    TEXT NOT NULL CHECK (service_kind IN ('attached', 'output')),
    service_id      UUID NOT NULL REFERENCES services(id),
    parent_service_id UUID REFERENCES services(id),
    function_id     TEXT NOT NULL,
    params_defaults JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS virtual_services_slug_idx ON virtual_services (slug);
CREATE INDEX IF NOT EXISTS virtual_services_service_id_idx ON virtual_services (service_id);
CREATE INDEX IF NOT EXISTS virtual_services_parent_service_id_idx ON virtual_services (parent_service_id);
