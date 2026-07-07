-- Datasets are getting their own REST resource view (GET /datasets/{id},
-- "much like an image service" per the console UI redesign) — a
-- human-readable description alongside name/format/CRS is basic to that,
-- and there was nowhere to store one before now.
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS description TEXT;
