-- ============================================================
-- InsiderTrack - Add insider_name and transaction_day columns
-- Migration: 002_add_insider_name_transaction_day.sql
-- Story: US-004 - Extract missing parser fields
-- ============================================================

-- Add insider_name to holdings (the name of the insider who filed the form)
ALTER TABLE holdings ADD COLUMN IF NOT EXISTS insider_name VARCHAR(255);

-- Add transaction_day to holdings (day-of-month extracted from operation date)
ALTER TABLE holdings ADD COLUMN IF NOT EXISTS transaction_day INT;

-- Index on insider_name for filtering/searching by insider
CREATE INDEX IF NOT EXISTS idx_holdings_insider_name ON holdings (insider_name);
