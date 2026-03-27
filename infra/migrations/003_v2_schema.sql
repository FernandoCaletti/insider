-- ============================================================
-- InsiderTrack - v2 Schema Migration
-- Migration: 003_v2_schema.sql
-- Story: US-005 - Database migration v2
-- ============================================================
-- Adds:
--   1. Missing insider_group column on holdings (used since US-001)
--   2. Missing delivery_date / status columns on documents
--   3. material_facts table (US-006, US-007, US-008)
--   4. alerts table (US-009, US-010, US-011)
--   5. financial_statements table (US-013, US-014, US-015)
--   6. dividends table (US-016)
--   7. insider_positions table (US-017, US-018)
--   8. RLS policies for all new tables
-- ============================================================

-- ============================================================
-- 1. Patch: holdings.insider_group (used in code, never migrated)
-- ============================================================
ALTER TABLE holdings ADD COLUMN IF NOT EXISTS insider_group VARCHAR(100);
CREATE INDEX IF NOT EXISTS idx_holdings_insider_group ON holdings (insider_group);

-- ============================================================
-- 2. Patch: documents columns expected by frontend Document type
-- ============================================================
ALTER TABLE documents ADD COLUMN IF NOT EXISTS delivery_date DATE;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'processed';

-- ============================================================
-- 3. Table: material_facts (CVM fatos relevantes)
-- ============================================================
CREATE TABLE IF NOT EXISTS material_facts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    reference_date DATE NOT NULL,
    category VARCHAR(100),
    subject TEXT,
    content TEXT,
    source_url TEXT,
    cvm_code VARCHAR(20),
    protocol VARCHAR(100) UNIQUE,
    delivery_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_material_facts_company ON material_facts (company_id);
CREATE INDEX IF NOT EXISTS idx_material_facts_date ON material_facts (reference_date);
CREATE INDEX IF NOT EXISTS idx_material_facts_company_date ON material_facts (company_id, reference_date);

-- ============================================================
-- 4. Table: alerts (system-generated atypical movement alerts)
-- ============================================================
CREATE TABLE IF NOT EXISTS alerts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    holding_id BIGINT REFERENCES holdings(id) ON DELETE SET NULL,
    alert_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL DEFAULT 'medium'
        CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    title VARCHAR(500) NOT NULL,
    description TEXT,
    metadata JSONB,
    is_read BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_company ON alerts (company_id);
CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts (alert_type);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts (severity);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_is_read ON alerts (is_read) WHERE is_read = FALSE;

-- ============================================================
-- 5. Table: financial_statements (CVM DFP/ITR data)
-- ============================================================
CREATE TABLE IF NOT EXISTS financial_statements (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    reference_date DATE NOT NULL,
    statement_type VARCHAR(50) NOT NULL,
    account_code VARCHAR(50),
    account_name VARCHAR(500),
    value DECIMAL(18,2),
    currency VARCHAR(10) DEFAULT 'BRL',
    source_url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (company_id, reference_date, statement_type, account_code)
);

CREATE INDEX IF NOT EXISTS idx_financial_company ON financial_statements (company_id);
CREATE INDEX IF NOT EXISTS idx_financial_date ON financial_statements (reference_date);
CREATE INDEX IF NOT EXISTS idx_financial_type ON financial_statements (statement_type);

-- ============================================================
-- 6. Table: dividends
-- ============================================================
CREATE TABLE IF NOT EXISTS dividends (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    ex_date DATE,
    payment_date DATE,
    record_date DATE,
    dividend_type VARCHAR(50),
    value_per_share DECIMAL(18,8),
    total_value DECIMAL(18,2),
    currency VARCHAR(10) DEFAULT 'BRL',
    source_url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dividends_company ON dividends (company_id);
CREATE INDEX IF NOT EXISTS idx_dividends_ex_date ON dividends (ex_date);
CREATE INDEX IF NOT EXISTS idx_dividends_company_ex ON dividends (company_id, ex_date);

-- ============================================================
-- 7. Table: insider_positions (individual insider position docs)
-- ============================================================
CREATE TABLE IF NOT EXISTS insider_positions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    insider_name VARCHAR(255) NOT NULL,
    insider_group VARCHAR(100),
    cpf_cnpj VARCHAR(20),
    reference_date DATE NOT NULL,
    asset_type VARCHAR(30),
    asset_description VARCHAR(500),
    quantity DECIMAL(18,2),
    total_value DECIMAL(18,2),
    source_url TEXT,
    document_id BIGINT REFERENCES documents(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_insider_pos_company ON insider_positions (company_id);
CREATE INDEX IF NOT EXISTS idx_insider_pos_insider ON insider_positions (insider_name);
CREATE INDEX IF NOT EXISTS idx_insider_pos_date ON insider_positions (reference_date);
CREATE INDEX IF NOT EXISTS idx_insider_pos_company_insider ON insider_positions (company_id, insider_name);

-- ============================================================
-- 8. Row Level Security for new tables
-- ============================================================

ALTER TABLE material_facts ENABLE ROW LEVEL SECURITY;
ALTER TABLE alerts ENABLE ROW LEVEL SECURITY;
ALTER TABLE financial_statements ENABLE ROW LEVEL SECURITY;
ALTER TABLE dividends ENABLE ROW LEVEL SECURITY;
ALTER TABLE insider_positions ENABLE ROW LEVEL SECURITY;

-- Anon: read-only
CREATE POLICY "anon_read_material_facts" ON material_facts FOR SELECT TO anon USING (TRUE);
CREATE POLICY "anon_read_alerts" ON alerts FOR SELECT TO anon USING (TRUE);
CREATE POLICY "anon_read_financial_statements" ON financial_statements FOR SELECT TO anon USING (TRUE);
CREATE POLICY "anon_read_dividends" ON dividends FOR SELECT TO anon USING (TRUE);
CREATE POLICY "anon_read_insider_positions" ON insider_positions FOR SELECT TO anon USING (TRUE);

-- Service role: full access
CREATE POLICY "service_role_all_material_facts" ON material_facts FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_role_all_alerts" ON alerts FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_role_all_financial_statements" ON financial_statements FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_role_all_dividends" ON dividends FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_role_all_insider_positions" ON insider_positions FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);

-- ============================================================
-- 9. Trigger: auto-update updated_at (reuse existing function)
-- ============================================================
-- No new tables have updated_at columns yet. The trigger function
-- update_updated_at_column() from 001 is available if needed.
