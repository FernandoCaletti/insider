-- ============================================================
-- InsiderTrack - Initial Database Schema
-- Migration: 001_initial_schema.sql
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================
-- Table: companies
-- ============================================================
CREATE TABLE IF NOT EXISTS companies (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cvm_code VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    cnpj VARCHAR(20),
    ticker VARCHAR(10),
    sector VARCHAR(100),
    subsector VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_companies_cvm_code ON companies (cvm_code);
CREATE INDEX IF NOT EXISTS idx_companies_ticker ON companies (ticker);
CREATE INDEX IF NOT EXISTS idx_companies_name_gin ON companies USING GIN (name gin_trgm_ops);

-- ============================================================
-- Table: documents
-- ============================================================
CREATE TABLE IF NOT EXISTS documents (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    reference_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    file_name VARCHAR(500),
    file_hash VARCHAR(64) UNIQUE,
    original_url TEXT,
    page_count INT,
    is_scanned BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (company_id, reference_date)
);

CREATE INDEX IF NOT EXISTS idx_documents_company_ref ON documents (company_id, reference_date);
CREATE INDEX IF NOT EXISTS idx_documents_year_month ON documents (year, month);
CREATE INDEX IF NOT EXISTS idx_documents_file_hash ON documents (file_hash);

-- ============================================================
-- Table: holdings
-- ============================================================
CREATE TABLE IF NOT EXISTS holdings (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    section VARCHAR(20) NOT NULL CHECK (section IN ('inicial', 'movimentacoes', 'final')),
    asset_type VARCHAR(30) NOT NULL,
    asset_description VARCHAR(500),
    quantity DECIMAL(18,2),
    unit_price DECIMAL(18,8),
    total_value DECIMAL(18,2),
    operation_type VARCHAR(20),
    operation_date DATE,
    broker VARCHAR(255),
    confidence VARCHAR(10) DEFAULT 'alta' CHECK (confidence IN ('alta', 'media', 'baixa')),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_holdings_document_id ON holdings (document_id);
CREATE INDEX IF NOT EXISTS idx_holdings_asset_type ON holdings (asset_type);
CREATE INDEX IF NOT EXISTS idx_holdings_section ON holdings (section);
CREATE INDEX IF NOT EXISTS idx_holdings_document_section ON holdings (document_id, section);
CREATE INDEX IF NOT EXISTS idx_holdings_operation_date ON holdings (operation_date);

-- ============================================================
-- Table: sync_log
-- ============================================================
CREATE TABLE IF NOT EXISTS sync_log (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    documents_found INT DEFAULT 0,
    documents_processed INT DEFAULT 0,
    documents_failed INT DEFAULT 0,
    error_details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- View: v_monthly_positions
-- Monthly position summary per company and asset type
-- ============================================================
CREATE OR REPLACE VIEW v_monthly_positions AS
SELECT
    c.id AS company_id,
    c.name AS company_name,
    c.ticker,
    d.year,
    d.month,
    d.reference_date,
    h.asset_type,
    h.asset_description,
    SUM(CASE WHEN h.section = 'inicial' THEN h.quantity ELSE 0 END) AS posicao_inicial,
    SUM(CASE WHEN h.section = 'final' THEN h.quantity ELSE 0 END) AS posicao_final,
    SUM(CASE WHEN h.section = 'inicial' THEN h.total_value ELSE 0 END) AS valor_inicial,
    SUM(CASE WHEN h.section = 'final' THEN h.total_value ELSE 0 END) AS valor_final
FROM holdings h
JOIN documents d ON d.id = h.document_id
JOIN companies c ON c.id = d.company_id
WHERE h.section IN ('inicial', 'final')
GROUP BY c.id, c.name, c.ticker, d.year, d.month, d.reference_date, h.asset_type, h.asset_description;

-- ============================================================
-- View: v_top_movements
-- Top movements excluding low confidence
-- ============================================================
CREATE OR REPLACE VIEW v_top_movements AS
SELECT
    h.id AS holding_id,
    c.id AS company_id,
    c.name AS company_name,
    c.ticker,
    d.reference_date,
    h.asset_type,
    h.asset_description,
    h.operation_type,
    h.operation_date,
    h.quantity,
    h.unit_price,
    h.total_value,
    h.broker,
    h.confidence
FROM holdings h
JOIN documents d ON d.id = h.document_id
JOIN companies c ON c.id = d.company_id
WHERE h.section = 'movimentacoes'
  AND h.confidence != 'baixa';

-- ============================================================
-- View: v_company_rankings
-- Aggregated company ranking data excluding low confidence
-- ============================================================
CREATE OR REPLACE VIEW v_company_rankings AS
SELECT
    c.id AS company_id,
    c.name AS company_name,
    c.ticker,
    h.operation_type,
    COUNT(*) AS total_operations,
    SUM(ABS(h.total_value)) AS total_value,
    SUM(h.quantity) AS total_quantity
FROM holdings h
JOIN documents d ON d.id = h.document_id
JOIN companies c ON c.id = d.company_id
WHERE h.section = 'movimentacoes'
  AND h.confidence != 'baixa'
GROUP BY c.id, c.name, c.ticker, h.operation_type;

-- ============================================================
-- Function: search_companies(term text)
-- Fuzzy search by name, ticker, and cvm_code
-- ============================================================
CREATE OR REPLACE FUNCTION search_companies(term TEXT)
RETURNS SETOF companies
LANGUAGE sql
STABLE
AS $$
    SELECT *
    FROM companies
    WHERE
        name ILIKE '%' || term || '%'
        OR ticker ILIKE '%' || term || '%'
        OR cvm_code ILIKE '%' || term || '%'
    ORDER BY
        CASE
            WHEN ticker ILIKE term THEN 0
            WHEN cvm_code = term THEN 1
            WHEN name ILIKE term || '%' THEN 2
            ELSE 3
        END,
        name ASC;
$$;

-- ============================================================
-- Row Level Security (RLS)
-- Public read for anon, write restricted to service_role
-- ============================================================

-- Enable RLS on all tables
ALTER TABLE companies ENABLE ROW LEVEL SECURITY;
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE holdings ENABLE ROW LEVEL SECURITY;
ALTER TABLE sync_log ENABLE ROW LEVEL SECURITY;

-- Anon: read-only access
CREATE POLICY "anon_read_companies" ON companies FOR SELECT TO anon USING (TRUE);
CREATE POLICY "anon_read_documents" ON documents FOR SELECT TO anon USING (TRUE);
CREATE POLICY "anon_read_holdings" ON holdings FOR SELECT TO anon USING (TRUE);
CREATE POLICY "anon_read_sync_log" ON sync_log FOR SELECT TO anon USING (TRUE);

-- Service role: full access (insert, update, delete)
CREATE POLICY "service_role_all_companies" ON companies FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_role_all_documents" ON documents FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_role_all_holdings" ON holdings FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY "service_role_all_sync_log" ON sync_log FOR ALL TO service_role USING (TRUE) WITH CHECK (TRUE);

-- ============================================================
-- Trigger: auto-update updated_at on companies
-- ============================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trigger_companies_updated_at
    BEFORE UPDATE ON companies
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
