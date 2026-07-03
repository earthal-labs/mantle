-- Append-only enforcement + LISTEN/NOTIFY for cache warmer (Phase 1B).

CREATE OR REPLACE FUNCTION mantle_reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'append-only catalog: % on % not permitted', TG_OP, TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS datasets_no_update ON datasets;
CREATE TRIGGER datasets_no_update
    BEFORE UPDATE OR DELETE ON datasets
    FOR EACH ROW EXECUTE PROCEDURE mantle_reject_mutation();

DROP TRIGGER IF EXISTS footprints_no_update ON footprints;
CREATE TRIGGER footprints_no_update
    BEFORE UPDATE OR DELETE ON footprints
    FOR EACH ROW EXECUTE PROCEDURE mantle_reject_mutation();

CREATE OR REPLACE FUNCTION mantle_notify_footprint_insert() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify(
        'mantle_footprint_insert',
        json_build_object(
            'footprint_id', NEW.id,
            'dataset_id', NEW.dataset_id,
            'partition_key', NEW.partition_key
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS footprints_insert_notify ON footprints;
CREATE TRIGGER footprints_insert_notify
    AFTER INSERT ON footprints
    FOR EACH ROW EXECUTE PROCEDURE mantle_notify_footprint_insert();
