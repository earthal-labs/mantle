-- Mantle catalog bootstrap (append-only mindset)
-- Postgres holds transaction boundaries; DuckLake Parquet mirrors footprint rows.

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS datasets (
    id              UUID PRIMARY KEY,
    name            TEXT NOT NULL,
    format          TEXT NOT NULL CHECK (format IN ('cog', 'icechunk')),
    storage_uri     TEXT NOT NULL,
    crs             TEXT,
    temporal_start  TIMESTAMPTZ,
    temporal_end    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS footprints (
    id              BIGSERIAL PRIMARY KEY,
    dataset_id      UUID NOT NULL REFERENCES datasets(id),
    geometry        GEOMETRY NOT NULL,
    cloud_cover     DOUBLE PRECISION,
    partition_key   TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS footprints_geometry_idx ON footprints USING GIST (geometry);
CREATE INDEX IF NOT EXISTS footprints_dataset_id_idx ON footprints (dataset_id);
CREATE INDEX IF NOT EXISTS footprints_partition_key_idx ON footprints (partition_key);

-- Append-only: no UPDATE/DELETE triggers enforced at application layer.
-- New versions create new Parquet files + new snapshot in DuckLake.
