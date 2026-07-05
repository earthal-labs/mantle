-- Soft-delete + deferred hard-purge for datasets.
--
-- The append-only trigger (002_append_only_notify.sql) blocks any UPDATE/DELETE
-- on datasets/footprints unconditionally. Rather than relax that broadly, we
-- add a separate, insert-mostly tombstone table that every read path checks,
-- and a narrow, session-scoped bypass used only by the scheduled purge job.

CREATE TABLE IF NOT EXISTS dataset_deletions (
    dataset_id  UUID PRIMARY KEY REFERENCES datasets(id),
    deleted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason      TEXT,
    purged_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS dataset_deletions_purge_pending_idx
    ON dataset_deletions (deleted_at)
    WHERE purged_at IS NULL;

-- virtual_services has no append-only trigger, so a real column + UPDATE works.
ALTER TABLE virtual_services ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

-- Narrow, explicit bypass for the purge job only. Ordinary app connections
-- never set `mantle.allow_purge`, so they remain fully blocked exactly as
-- before; only a connection that explicitly opts in via
-- `SET LOCAL mantle.allow_purge = 'on'` (scoped to its own transaction) can
-- mutate datasets/footprints.
CREATE OR REPLACE FUNCTION mantle_reject_mutation() RETURNS trigger AS $$
BEGIN
    IF current_setting('mantle.allow_purge', true) = 'on' THEN
        RETURN COALESCE(NEW, OLD);
    END IF;
    RAISE EXCEPTION 'append-only catalog: % on % not permitted', TG_OP, TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;
