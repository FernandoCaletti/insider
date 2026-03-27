-- Migration 006: Materialized views for expensive ranking queries
-- These pre-compute common aggregations and can be refreshed after syncs.

-- Top buyers: companies ranked by total buy value (all-time, no filters)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_rankings_top_buyers AS
SELECT
    c.id AS company_id,
    c.name AS company_name,
    c.ticker,
    h.operation_date,
    h.insider_group,
    COUNT(*) AS op_count,
    COALESCE(SUM(h.total_value), 0) AS total_value,
    COALESCE(SUM(h.quantity), 0) AS total_quantity
FROM holdings h
JOIN documents d ON d.id = h.document_id
JOIN companies c ON c.id = d.company_id
WHERE h.section = 'movimentacoes'
  AND h.confidence != 'baixa'
  AND h.operation_type ILIKE 'Compra%'
GROUP BY c.id, c.name, c.ticker, h.operation_date, h.insider_group;

CREATE INDEX IF NOT EXISTS idx_mv_top_buyers_date ON mv_rankings_top_buyers (operation_date);
CREATE INDEX IF NOT EXISTS idx_mv_top_buyers_group ON mv_rankings_top_buyers (insider_group);

-- Top sellers: companies ranked by total sell value (all-time, no filters)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_rankings_top_sellers AS
SELECT
    c.id AS company_id,
    c.name AS company_name,
    c.ticker,
    h.operation_date,
    h.insider_group,
    COUNT(*) AS op_count,
    COALESCE(SUM(ABS(h.total_value)), 0) AS total_value,
    COALESCE(SUM(h.quantity), 0) AS total_quantity
FROM holdings h
JOIN documents d ON d.id = h.document_id
JOIN companies c ON c.id = d.company_id
WHERE h.section = 'movimentacoes'
  AND h.confidence != 'baixa'
  AND h.operation_type ILIKE 'Venda%'
GROUP BY c.id, c.name, c.ticker, h.operation_date, h.insider_group;

CREATE INDEX IF NOT EXISTS idx_mv_top_sellers_date ON mv_rankings_top_sellers (operation_date);
CREATE INDEX IF NOT EXISTS idx_mv_top_sellers_group ON mv_rankings_top_sellers (insider_group);

-- Most active: companies ranked by operation count (all-time, no filters)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_rankings_most_active AS
SELECT
    c.id AS company_id,
    c.name AS company_name,
    c.ticker,
    h.operation_date,
    h.insider_group,
    COUNT(*) AS op_count,
    COALESCE(SUM(ABS(h.total_value)), 0) AS total_value
FROM holdings h
JOIN documents d ON d.id = h.document_id
JOIN companies c ON c.id = d.company_id
WHERE h.section = 'movimentacoes'
  AND h.confidence != 'baixa'
GROUP BY c.id, c.name, c.ticker, h.operation_date, h.insider_group;

CREATE INDEX IF NOT EXISTS idx_mv_most_active_date ON mv_rankings_most_active (operation_date);
CREATE INDEX IF NOT EXISTS idx_mv_most_active_group ON mv_rankings_most_active (insider_group);

-- By role: rankings grouped by insider_group
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_rankings_by_role AS
SELECT
    h.insider_group,
    h.operation_date,
    COUNT(*) AS op_count,
    COALESCE(SUM(ABS(h.total_value)), 0) AS total_value,
    COUNT(DISTINCT d.company_id) AS companies_count,
    COALESCE(SUM(CASE WHEN h.operation_type ILIKE 'Compra%' THEN 1 ELSE 0 END), 0) AS buy_count,
    COALESCE(SUM(CASE WHEN h.operation_type ILIKE 'Venda%' THEN 1 ELSE 0 END), 0) AS sell_count
FROM holdings h
JOIN documents d ON d.id = h.document_id
WHERE h.section = 'movimentacoes'
  AND h.confidence != 'baixa'
  AND h.insider_group IS NOT NULL
GROUP BY h.insider_group, h.operation_date;

CREATE INDEX IF NOT EXISTS idx_mv_by_role_date ON mv_rankings_by_role (operation_date);

-- By broker: rankings grouped by broker
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_rankings_by_broker AS
SELECT
    h.broker,
    h.operation_date,
    h.insider_group,
    COUNT(*) AS op_count,
    COALESCE(SUM(ABS(h.total_value)), 0) AS total_value,
    COUNT(DISTINCT d.company_id) AS companies_count,
    COALESCE(SUM(CASE WHEN h.operation_type ILIKE 'Compra%' THEN 1 ELSE 0 END), 0) AS buy_count,
    COALESCE(SUM(CASE WHEN h.operation_type ILIKE 'Venda%' THEN 1 ELSE 0 END), 0) AS sell_count
FROM holdings h
JOIN documents d ON d.id = h.document_id
WHERE h.section = 'movimentacoes'
  AND h.confidence != 'baixa'
  AND h.broker IS NOT NULL
  AND h.broker != ''
GROUP BY h.broker, h.operation_date, h.insider_group;

CREATE INDEX IF NOT EXISTS idx_mv_by_broker_date ON mv_rankings_by_broker (operation_date);
CREATE INDEX IF NOT EXISTS idx_mv_by_broker_group ON mv_rankings_by_broker (insider_group);

-- Dashboard summary: pre-computed counts
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dashboard_summary AS
SELECT
    (SELECT COUNT(*) FROM companies) AS total_companies,
    (SELECT COUNT(*) FROM documents) AS total_documents,
    (SELECT COUNT(*) FROM holdings WHERE section = 'movimentacoes' AND confidence != 'baixa') AS total_movements,
    (SELECT MIN(reference_date) FROM documents) AS date_min,
    (SELECT MAX(reference_date) FROM documents) AS date_max;
