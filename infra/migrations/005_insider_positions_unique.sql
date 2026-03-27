-- ============================================================
-- InsiderTrack - Insider Positions UNIQUE Constraint
-- Migration: 005_insider_positions_unique.sql
-- Story: US-017 - Individual position collector
-- ============================================================
-- Adds a UNIQUE constraint on insider_positions for idempotent
-- ON CONFLICT upserts. Uses (company_id, insider_name,
-- reference_date, asset_type) as the composite key.
-- Sets asset_type NOT NULL with default '' to support the constraint.
-- ============================================================

-- Ensure no NULLs in asset_type before adding NOT NULL constraint
UPDATE insider_positions SET asset_type = '' WHERE asset_type IS NULL;

ALTER TABLE insider_positions
    ALTER COLUMN asset_type SET DEFAULT '',
    ALTER COLUMN asset_type SET NOT NULL;

ALTER TABLE insider_positions
    ADD CONSTRAINT insider_positions_unique
    UNIQUE (company_id, insider_name, reference_date, asset_type);
