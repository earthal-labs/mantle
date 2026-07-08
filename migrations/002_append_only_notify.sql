-- Append-only enforcement + LISTEN/NOTIFY for cache warmer (Phase 1B).

CREATE OR REPLACE FUNCTION mantle_reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'append-only catalog: % on % not permitted', TG_OP, TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS services_no_update ON services;
CREATE TRIGGER services_no_update
    BEFORE UPDATE OR DELETE ON services
    FOR EACH ROW EXECUTE PROCEDURE mantle_reject_mutation();

DROP TRIGGER IF EXISTS scenes_no_update ON scenes;
CREATE TRIGGER scenes_no_update
    BEFORE UPDATE OR DELETE ON scenes
    FOR EACH ROW EXECUTE PROCEDURE mantle_reject_mutation();

DROP TRIGGER IF EXISTS service_assets_no_update ON service_assets;
CREATE TRIGGER service_assets_no_update
    BEFORE UPDATE OR DELETE ON service_assets
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
            'scene_id', NEW.scene_id,
            'service_id', NEW.service_id,
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
