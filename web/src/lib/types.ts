// Company types
export interface Company {
  id: number;
  cvm_code: string;
  name: string;
  cnpj: string | null;
  ticker: string | null;
  sector: string | null;
  subsector: string | null;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface CompanyListItem extends Company {
  total_documents: number;
  last_document: string | null;
}

export interface CompanyDetail extends Company {
  current_positions: Holding[];
}

// Document types
export interface Document {
  id: number;
  company_id: number;
  reference_date: string;
  delivery_date: string | null;
  file_hash: string;
  filename: string | null;
  source_url: string | null;
  year: number;
  month: number;
  status: string;
  created_at: string;
}

// Holding types
export type AssetType =
  | "ACAO_ON"
  | "ACAO_PN"
  | "DEBENTURE"
  | "OPCAO"
  | "OPCAO_COMPRA"
  | "OPCAO_VENDA"
  | "BDR"
  | "UNIT"
  | "OUTRO";

export type Section = "inicial" | "movimentacoes" | "final";

export type Confidence = "alta" | "media" | "baixa";

export interface Holding {
  id: number;
  document_id: number;
  section: Section;
  asset_type: AssetType;
  asset_description: string | null;
  quantity: number;
  unit_price: number | null;
  total_value: number | null;
  operation_type: string | null;
  operation_date: string | null;
  broker: string | null;
  confidence: Confidence;
  insider_group: string | null;
  company_name?: string;
  company_ticker?: string;
  company_id?: number;
  reference_date?: string;
}

// Pagination
export interface PaginatedResponse<T> {
  data: T[];
  total: number;
  page: number;
  per_page: number;
}

// Holdings list response
export interface HoldingsResponse extends PaginatedResponse<Holding> {
  filters_applied: Record<string, string>;
}

// Dashboard types
export interface DashboardSummary {
  total_companies: number;
  total_documents: number;
  total_movements: number;
  last_sync: SyncStatus | null;
  data_range: {
    min_date: string | null;
    max_date: string | null;
  };
}

export interface RecentMovement {
  id: number;
  company_name: string;
  company_ticker: string | null;
  company_id: number;
  asset_type: AssetType;
  asset_description: string | null;
  operation_type: string;
  quantity: number;
  total_value: number;
  operation_date: string;
}

export interface MovementChartPoint {
  date: string;
  compras: number;
  vendas: number;
  valor_compras: number;
  valor_vendas: number;
}

// Rankings types
export interface RankingEntry {
  company_id: number;
  company_name: string;
  company_ticker: string | null;
  total_operations: number;
  total_value: number;
  total_quantity: number;
}

export interface LargestPosition {
  company_id: number;
  company_name: string;
  company_ticker: string | null;
  asset_type: AssetType;
  asset_description: string | null;
  total_quantity: number;
  estimated_value: number | null;
}

// Sync types
export interface SyncStatus {
  id: number;
  started_at: string;
  finished_at: string | null;
  status: "running" | "success" | "error";
  documents_found: number;
  documents_processed: number;
  documents_failed: number;
  error_details: Record<string, unknown> | null;
}

// Position history
export interface PositionHistoryPoint {
  month: string;
  posicao_inicial: number;
  posicao_final: number;
}

// Correlation types
export interface CorrelationEntry {
  holding_id: number;
  operation_date: string;
  operation_type: string | null;
  asset_type: AssetType;
  total_value: number | null;
  quantity: number | null;
  insider_group: string | null;
  material_fact_id: number;
  fact_date: string;
  fact_category: string | null;
  fact_subject: string | null;
  days_diff: number;
  company_id: number;
  company_name: string;
  company_ticker: string | null;
}

export interface CorrelationSummary {
  total_correlations: number;
  companies_involved: number;
  unique_movements: number;
  unique_facts: number;
  total_value: number;
  avg_days_diff: number;
  movements_before_fact: number;
  movements_after_fact: number;
}

// Alert types
export type AlertType = 'alto_valor' | 'volume_atipico' | 'mudanca_direcao' | 'retorno_atividade';
export type AlertSeverity = 'low' | 'medium' | 'high' | 'critical';

export interface Alert {
  id: number;
  company_id: number;
  holding_id: number | null;
  alert_type: AlertType;
  severity: AlertSeverity;
  title: string;
  description: string | null;
  metadata: Record<string, unknown> | null;
  is_read: boolean;
  created_at: string;
  company_name?: string;
  company_ticker?: string | null;
}

export interface AlertsSummary {
  total: number;
  unread: number;
  by_type: { alert_type: string; count: number }[];
  by_severity: { severity: string; count: number }[];
}

export interface TopCorrelatedCompany {
  company_id: number;
  company_name: string;
  company_ticker: string | null;
  correlation_count: number;
  unique_movements: number;
  unique_facts: number;
  total_value: number;
  avg_days_diff: number;
}
