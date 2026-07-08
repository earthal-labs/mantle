-- Mantle catalog bootstrap (append-only mindset)
-- Postgres holds transaction boundaries; DuckLake Parquet mirrors footprint rows.
--
-- Hierarchy: a service is a pure container (e.g. "Landsat 9 Collection 2").
-- Each service has one or more scenes (one spatiotemporal acquisition, the
-- STAC Item equivalent). Each scene has one or more assets (the actual
-- raster files — one band each, the STAC Asset equivalent). A plain
-- single-file upload is just the degenerate case: one scene, one asset.

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS services (
    id              UUID PRIMARY KEY,
    slug            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    description     TEXT,
    format          TEXT NOT NULL CHECK (format IN ('cog', 'icechunk')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scenes (
    id              UUID PRIMARY KEY,
    service_id      UUID NOT NULL REFERENCES services(id),
    label           TEXT,
    acquired_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS scenes_service_id_idx ON scenes (service_id);

CREATE TABLE IF NOT EXISTS service_assets (
    id              UUID PRIMARY KEY,
    service_id      UUID NOT NULL REFERENCES services(id),
    scene_id        UUID NOT NULL REFERENCES scenes(id),
    band_role       TEXT NOT NULL,
    band_index      INTEGER NOT NULL DEFAULT 1,
    format          TEXT NOT NULL CHECK (format IN ('cog', 'icechunk')),
    storage_uri     TEXT NOT NULL,
    crs             TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS service_assets_scene_role_idx ON service_assets (scene_id, band_role);
CREATE INDEX IF NOT EXISTS service_assets_service_id_idx ON service_assets (service_id);
CREATE INDEX IF NOT EXISTS service_assets_scene_id_idx ON service_assets (scene_id);

CREATE TABLE IF NOT EXISTS footprints (
    id              BIGSERIAL PRIMARY KEY,
    scene_id        UUID NOT NULL REFERENCES scenes(id),
    service_id      UUID NOT NULL REFERENCES services(id),
    geometry        GEOMETRY NOT NULL,
    cloud_cover     DOUBLE PRECISION,
    partition_key   TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS footprints_geometry_idx ON footprints USING GIST (geometry);
CREATE INDEX IF NOT EXISTS footprints_scene_id_idx ON footprints (scene_id);
CREATE INDEX IF NOT EXISTS footprints_service_id_idx ON footprints (service_id);
CREATE INDEX IF NOT EXISTS footprints_partition_key_idx ON footprints (partition_key);

-- Append-only: no UPDATE/DELETE triggers enforced at application layer.
-- New versions create new Parquet files + new snapshot in DuckLake.
