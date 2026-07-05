-- dataset_deletions is a permanent audit record: it must survive purging the
-- dataset it refers to (deleted_at + purged_at together tell the whole
-- story). A foreign key to datasets(id) makes that impossible -- Postgres
-- refuses `DELETE FROM datasets` while the (intentionally-persisting)
-- tombstone row still references it. Drop the constraint; the row is only
-- ever inserted after confirming the dataset exists (see
-- soft_delete_dataset), so this never allowed orphaned garbage in anyway.
ALTER TABLE dataset_deletions DROP CONSTRAINT IF EXISTS dataset_deletions_dataset_id_fkey;
