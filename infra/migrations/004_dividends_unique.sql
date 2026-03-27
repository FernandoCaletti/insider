-- ============================================================
-- InsiderTrack - Dividends unique constraint
-- Migration: 004_dividends_unique.sql
-- Story: US-016 - Dividends collector
-- ============================================================
-- Adds UNIQUE constraint on (company_id, ex_date, dividend_type)
-- for idempotent CVM dividend imports via ON CONFLICT.
-- ============================================================

ALTER TABLE dividends
    ADD CONSTRAINT uq_dividends_company_ex_type
    UNIQUE (company_id, ex_date, dividend_type);
