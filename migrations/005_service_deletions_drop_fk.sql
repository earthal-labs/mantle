-- service_deletions is a permanent audit record: it must survive purging the
-- service it refers to (deleted_at + purged_at together tell the whole
-- story). A foreign key to services(id) makes that impossible -- Postgres
-- refuses `DELETE FROM services` while the (intentionally-persisting)
-- tombstone row still references it. Drop the constraint; the row is only
-- ever inserted after confirming the service exists (see
-- soft_delete_service), so this never allowed orphaned garbage in anyway.
ALTER TABLE service_deletions DROP CONSTRAINT IF EXISTS service_deletions_service_id_fkey;
