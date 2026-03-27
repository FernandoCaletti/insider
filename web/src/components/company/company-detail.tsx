"use client";

import { useState, useEffect, useCallback } from "react";
import { Download, FileText, ExternalLink, DollarSign } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card, CardContent } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api";
import {
  formatCurrency,
  formatDate,
  formatQuantity,
  displayTicker,
} from "@/lib/utils";
import type {
  CompanyDetail,
  Holding,
  Document,
  PositionHistoryPoint,
  PaginatedResponse,
  FinancialStatement,
  Dividend,
} from "@/lib/types";
import { PositionChart } from "./position-chart";

const ASSET_TYPES = [
  { value: "ACAO_ON", label: "Ação ON" },
  { value: "ACAO_PN", label: "Ação PN" },
  { value: "DEBENTURE", label: "Debenture" },
  { value: "OPCAO", label: "Opção" },
  { value: "OPCAO_COMPRA", label: "Opção de Compra" },
  { value: "OPCAO_VENDA", label: "Opção de Venda" },
  { value: "BDR", label: "BDR" },
  { value: "UNIT", label: "Unit" },
  { value: "OUTRO", label: "Outro" },
];

const INSIDER_GROUPS = [
  { value: "Controlador", label: "Controlador" },
  { value: "Conselho de Administracao", label: "Conselho de Administração" },
  { value: "Diretoria", label: "Diretoria" },
  { value: "Conselho Fiscal", label: "Conselho Fiscal" },
  { value: "Orgaos Tecnicos", label: "Órgãos Técnicos" },
  { value: "Pessoas Ligadas", label: "Pessoas Ligadas" },
];

function formatCNPJ(cnpj: string | null): string {
  if (!cnpj) return "\u2014";
  const digits = cnpj.replace(/\D/g, "");
  if (digits.length !== 14) return cnpj;
  return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8, 12)}-${digits.slice(12)}`;
}

function CompanyHeader({ company }: { company: CompanyDetail }) {
  return (
    <div className="space-y-1">
      <div className="flex items-center gap-3">
        <h1 className="text-2xl font-bold">{company.name}</h1>
        <Badge variant={company.is_active ? "success" : "secondary"}>
          {company.is_active ? "Ativa" : "Inativa"}
        </Badge>
      </div>
      <div className="flex flex-wrap gap-x-6 gap-y-1 text-sm text-muted-foreground">
        <span>
          Ticker:{" "}
          <span className="font-mono font-medium text-foreground">
            {displayTicker(company.ticker)}
          </span>
        </span>
        <span>
          CNPJ:{" "}
          <span className="font-mono font-medium text-foreground">
            {formatCNPJ(company.cnpj)}
          </span>
        </span>
        <span>
          Setor:{" "}
          <span className="font-medium text-foreground">
            {company.sector || "\u2014"}
          </span>
        </span>
      </div>
    </div>
  );
}

function PositionsTab({ positions }: { positions: Holding[] }) {
  if (positions.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
        <p className="text-lg">Nenhuma posição encontrada</p>
        <p className="text-sm">
          Esta empresa não possui posições no último documento.
        </p>
      </div>
    );
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Grupo</TableHead>
          <TableHead>Tipo</TableHead>
          <TableHead>Descrição</TableHead>
          <TableHead className="text-right">Quantidade</TableHead>
          <TableHead className="text-right">Valor</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {positions.map((pos, i) => (
          <TableRow key={i}>
            <TableCell className="text-sm text-muted-foreground">
              {pos.insider_group || "\u2014"}
            </TableCell>
            <TableCell className="font-medium">{pos.asset_type}</TableCell>
            <TableCell>{pos.asset_description || "\u2014"}</TableCell>
            <TableCell className="text-right font-mono">
              {formatQuantity(pos.quantity)}
            </TableCell>
            <TableCell className="text-right font-mono">
              {pos.total_value != null
                ? formatCurrency(pos.total_value)
                : "\u2014"}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

function MovementsTab({ companyId }: { companyId: number }) {
  const [data, setData] = useState<Holding[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const perPage = 20;
  const [loading, setLoading] = useState(true);
  const [assetType, setAssetType] = useState("");
  const [operationType, setOperationType] = useState("");
  const [insiderGroup, setInsiderGroup] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");

  const fetchMovements = useCallback(async () => {
    setLoading(true);
    try {
      const params: Record<string, string | number> = {
        section: "movimentacoes",
        page,
        per_page: perPage,
      };
      if (assetType) params.asset_type = assetType;
      if (operationType) params.operation_type = operationType;
      if (insiderGroup) params.insider_group = insiderGroup;
      if (dateFrom) params.date_from = dateFrom;
      if (dateTo) params.date_to = dateTo;

      const result = await api.get<PaginatedResponse<Holding>>(
        `/companies/${companyId}/holdings`,
        { params }
      );
      setData(result.data);
      setTotal(result.total);
    } catch {
      setData([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  }, [companyId, page, assetType, operationType, insiderGroup, dateFrom, dateTo]);

  useEffect(() => {
    fetchMovements();
  }, [fetchMovements]);

  const handleExportCSV = () => {
    const baseUrl =
      process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";
    const params = new URLSearchParams();
    params.set("company_id", String(companyId));
    params.set("section", "movimentacoes");
    if (assetType) params.set("asset_type", assetType);
    if (operationType) params.set("operation_type", operationType);
    if (insiderGroup) params.set("insider_group", insiderGroup);
    if (dateFrom) params.set("date_from", dateFrom);
    if (dateTo) params.set("date_to", dateTo);
    window.open(`${baseUrl}/holdings/export?${params.toString()}`, "_blank");
  };

  const totalPages = Math.ceil(total / perPage);

  return (
    <div className="space-y-4">
      <div className="flex flex-col sm:flex-row gap-3">
        <Select
          value={assetType || "all"}
          onValueChange={(v) => {
            setAssetType(v === "all" ? "" : v);
            setPage(1);
          }}
        >
          <SelectTrigger className="w-full sm:w-[180px]">
            <SelectValue placeholder="Tipo de ativo" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">Todos os ativos</SelectItem>
            {ASSET_TYPES.map((t) => (
              <SelectItem key={t.value} value={t.value}>
                {t.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select
          value={operationType || "all"}
          onValueChange={(v) => {
            setOperationType(v === "all" ? "" : v);
            setPage(1);
          }}
        >
          <SelectTrigger className="w-full sm:w-[160px]">
            <SelectValue placeholder="Operação" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">Todas</SelectItem>
            <SelectItem value="Compra">Compra</SelectItem>
            <SelectItem value="Venda">Venda</SelectItem>
          </SelectContent>
        </Select>

        <Select
          value={insiderGroup || "all"}
          onValueChange={(v) => {
            setInsiderGroup(v === "all" ? "" : v);
            setPage(1);
          }}
        >
          <SelectTrigger className="w-full sm:w-[220px]">
            <SelectValue placeholder="Grupo do Insider" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">Todos os grupos</SelectItem>
            {INSIDER_GROUPS.map((g) => (
              <SelectItem key={g.value} value={g.value}>
                {g.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Input
          type="date"
          value={dateFrom}
          onChange={(e) => {
            setDateFrom(e.target.value);
            setPage(1);
          }}
          className="w-full sm:w-[160px]"
        />
        <Input
          type="date"
          value={dateTo}
          onChange={(e) => {
            setDateTo(e.target.value);
            setPage(1);
          }}
          className="w-full sm:w-[160px]"
        />

        <Button
          variant="outline"
          onClick={handleExportCSV}
          className="whitespace-nowrap"
        >
          <Download className="mr-2 h-4 w-4" />
          Exportar CSV
        </Button>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-16 text-muted-foreground">
          Carregando...
        </div>
      ) : data.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <p className="text-lg">Nenhuma movimentação encontrada</p>
        </div>
      ) : (
        <>
          <div className="text-sm text-muted-foreground">
            {formatQuantity(total)} movimentac
            {total !== 1 ? "oes" : "ao"} encontrada
            {total !== 1 ? "s" : ""}
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Data</TableHead>
                <TableHead>Ativo</TableHead>
                <TableHead>Operação</TableHead>
                <TableHead>Grupo</TableHead>
                <TableHead className="text-right">Quantidade</TableHead>
                <TableHead className="text-right">Preço Unit.</TableHead>
                <TableHead className="text-right">Valor Total</TableHead>
                <TableHead>Corretora</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((h) => (
                <TableRow key={h.id}>
                  <TableCell>
                    {h.operation_date
                      ? formatDate(h.operation_date)
                      : h.reference_date
                        ? formatDate(h.reference_date)
                        : "\u2014"}
                  </TableCell>
                  <TableCell>
                    <span className="font-medium">{h.asset_type}</span>
                    {h.asset_description && (
                      <span className="text-sm text-muted-foreground ml-1">
                        - {h.asset_description}
                      </span>
                    )}
                  </TableCell>
                  <TableCell>
                    {h.operation_type ? (
                      <Badge
                        variant={
                          h.operation_type === "Compra"
                            ? "success"
                            : "destructive"
                        }
                      >
                        {h.operation_type}
                      </Badge>
                    ) : (
                      "\u2014"
                    )}
                  </TableCell>
                  <TableCell className="text-sm">
                    {h.insider_group || "\u2014"}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {formatQuantity(h.quantity)}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {h.unit_price != null
                      ? formatCurrency(h.unit_price)
                      : "\u2014"}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {h.total_value != null
                      ? formatCurrency(Math.abs(h.total_value))
                      : "\u2014"}
                  </TableCell>
                  <TableCell className="text-sm">
                    {h.broker || "\u2014"}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>

          {totalPages > 1 && (
            <div className="flex items-center justify-center gap-2 pt-4">
              <Button
                variant="outline"
                size="sm"
                disabled={page <= 1}
                onClick={() => setPage(page - 1)}
              >
                Anterior
              </Button>
              <span className="text-sm text-muted-foreground">
                Página {page} de {totalPages}
              </span>
              <Button
                variant="outline"
                size="sm"
                disabled={page >= totalPages}
                onClick={() => setPage(page + 1)}
              >
                Próxima
              </Button>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function EvolutionTab({ companyId }: { companyId: number }) {
  const [assetType, setAssetType] = useState("");
  const [data, setData] = useState<PositionHistoryPoint[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      try {
        const params: Record<string, string | number> = { months: 12 };
        if (assetType) params.asset_type = assetType;
        const result = await api.get<{ data: PositionHistoryPoint[] }>(
          `/companies/${companyId}/position-history`,
          { params }
        );
        if (!cancelled) setData(result.data);
      } catch {
        if (!cancelled) setData([]);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [companyId, assetType]);

  return (
    <div className="space-y-4">
      <Select
        value={assetType || "all"}
        onValueChange={(v) => setAssetType(v === "all" ? "" : v)}
      >
        <SelectTrigger className="w-full sm:w-[200px]">
          <SelectValue placeholder="Tipo de ativo" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="all">Todos os ativos</SelectItem>
          {ASSET_TYPES.map((t) => (
            <SelectItem key={t.value} value={t.value}>
              {t.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {loading ? (
        <div className="flex items-center justify-center py-16 text-muted-foreground">
          Carregando...
        </div>
      ) : data.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <p className="text-lg">Sem dados de evolução</p>
        </div>
      ) : (
        <Card>
          <CardContent className="pt-6">
            <PositionChart data={data} />
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function DocumentsTab({ companyId }: { companyId: number }) {
  const [documents, setDocuments] = useState<Document[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const perPage = 20;
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      try {
        const result = await api.get<PaginatedResponse<Document>>(
          `/companies/${companyId}/documents`,
          { params: { page, per_page: perPage } }
        );
        if (!cancelled) {
          setDocuments(result.data);
          setTotal(result.total);
        }
      } catch {
        if (!cancelled) {
          setDocuments([]);
          setTotal(0);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [companyId, page]);

  const totalPages = Math.ceil(total / perPage);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-16 text-muted-foreground">
        Carregando...
      </div>
    );
  }

  if (documents.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
        <FileText className="h-12 w-12 mb-3" />
        <p className="text-lg">Nenhum documento encontrado</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="relative ml-4 border-l-2 border-border pl-8 space-y-8">
        {documents.map((doc) => (
          <div key={doc.id} className="relative">
            <div className="absolute -left-[calc(2rem+5px)] top-1.5 h-2.5 w-2.5 rounded-full bg-primary" />
            <div className="space-y-1">
              <div className="flex items-center gap-3">
                <span className="font-medium">
                  {formatDate(doc.reference_date)}
                </span>
                <Badge variant="secondary" className="text-xs">
                  {doc.status}
                </Badge>
              </div>
              <p className="text-sm text-muted-foreground">
                {doc.filename || `Documento ${doc.id}`}
              </p>
              {doc.source_url && (
                <a
                  href={doc.source_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 text-sm text-primary hover:underline"
                >
                  <ExternalLink className="h-3 w-3" />
                  Ver PDF original na CVM
                </a>
              )}
            </div>
          </div>
        ))}
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-center gap-2 pt-4">
          <Button
            variant="outline"
            size="sm"
            disabled={page <= 1}
            onClick={() => setPage(page - 1)}
          >
            Anterior
          </Button>
          <span className="text-sm text-muted-foreground">
            Página {page} de {totalPages}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= totalPages}
            onClick={() => setPage(page + 1)}
          >
            Próxima
          </Button>
        </div>
      )}
    </div>
  );
}

const STATEMENT_TYPES = [
  { value: "BPA", label: "Ativo (BPA)" },
  { value: "BPP", label: "Passivo (BPP)" },
  { value: "DRE", label: "DRE" },
  { value: "DFC_MI", label: "Fluxo de Caixa (DFC)" },
];

function FinancialTab({ companyId }: { companyId: number }) {
  const [statements, setStatements] = useState<FinancialStatement[]>([]);
  const [statementsTotal, setStatementsTotal] = useState(0);
  const [statementsPage, setStatementsPage] = useState(1);
  const [statementsLoading, setStatementsLoading] = useState(true);
  const [statementType, setStatementType] = useState("");
  const [statementsDateFrom, setStatementsDateFrom] = useState("");
  const [statementsDateTo, setStatementsDateTo] = useState("");

  const [dividends, setDividends] = useState<Dividend[]>([]);
  const [dividendsTotal, setDividendsTotal] = useState(0);
  const [dividendsPage, setDividendsPage] = useState(1);
  const [dividendsLoading, setDividendsLoading] = useState(true);
  const [dividendsDateFrom, setDividendsDateFrom] = useState("");
  const [dividendsDateTo, setDividendsDateTo] = useState("");

  const perPage = 20;

  const fetchStatements = useCallback(async () => {
    setStatementsLoading(true);
    try {
      const params: Record<string, string | number> = {
        page: statementsPage,
        per_page: perPage,
      };
      if (statementType) params.statement_type = statementType;
      if (statementsDateFrom) params.date_from = statementsDateFrom;
      if (statementsDateTo) params.date_to = statementsDateTo;

      const result = await api.get<PaginatedResponse<FinancialStatement>>(
        `/companies/${companyId}/financial-statements`,
        { params }
      );
      setStatements(result.data);
      setStatementsTotal(result.total);
    } catch {
      setStatements([]);
      setStatementsTotal(0);
    } finally {
      setStatementsLoading(false);
    }
  }, [companyId, statementsPage, statementType, statementsDateFrom, statementsDateTo]);

  const fetchDividends = useCallback(async () => {
    setDividendsLoading(true);
    try {
      const params: Record<string, string | number> = {
        page: dividendsPage,
        per_page: perPage,
      };
      if (dividendsDateFrom) params.date_from = dividendsDateFrom;
      if (dividendsDateTo) params.date_to = dividendsDateTo;

      const result = await api.get<PaginatedResponse<Dividend>>(
        `/companies/${companyId}/dividends`,
        { params }
      );
      setDividends(result.data);
      setDividendsTotal(result.total);
    } catch {
      setDividends([]);
      setDividendsTotal(0);
    } finally {
      setDividendsLoading(false);
    }
  }, [companyId, dividendsPage, dividendsDateFrom, dividendsDateTo]);

  useEffect(() => {
    fetchStatements();
  }, [fetchStatements]);

  useEffect(() => {
    fetchDividends();
  }, [fetchDividends]);

  const statementsTotalPages = Math.ceil(statementsTotal / perPage);
  const dividendsTotalPages = Math.ceil(dividendsTotal / perPage);

  return (
    <div className="space-y-8">
      {/* Financial Statements Section */}
      <div className="space-y-4">
        <h3 className="text-lg font-semibold">Demonstrações Financeiras</h3>
        <div className="flex flex-col sm:flex-row gap-3">
          <Select
            value={statementType || "all"}
            onValueChange={(v) => {
              setStatementType(v === "all" ? "" : v);
              setStatementsPage(1);
            }}
          >
            <SelectTrigger className="w-full sm:w-[200px]">
              <SelectValue placeholder="Tipo" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todos os tipos</SelectItem>
              {STATEMENT_TYPES.map((t) => (
                <SelectItem key={t.value} value={t.value}>
                  {t.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Input
            type="date"
            value={statementsDateFrom}
            onChange={(e) => {
              setStatementsDateFrom(e.target.value);
              setStatementsPage(1);
            }}
            className="w-full sm:w-[160px]"
          />
          <Input
            type="date"
            value={statementsDateTo}
            onChange={(e) => {
              setStatementsDateTo(e.target.value);
              setStatementsPage(1);
            }}
            className="w-full sm:w-[160px]"
          />
        </div>

        {statementsLoading ? (
          <div className="flex items-center justify-center py-12 text-muted-foreground">
            Carregando...
          </div>
        ) : statements.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
            <DollarSign className="h-12 w-12 mb-3" />
            <p className="text-lg">Nenhuma demonstração financeira encontrada</p>
          </div>
        ) : (
          <>
            <div className="text-sm text-muted-foreground">
              {formatQuantity(statementsTotal)} registro
              {statementsTotal !== 1 ? "s" : ""}
            </div>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Data Ref.</TableHead>
                  <TableHead>Tipo</TableHead>
                  <TableHead>Código</TableHead>
                  <TableHead>Conta</TableHead>
                  <TableHead className="text-right">Valor</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {statements.map((fs) => (
                  <TableRow key={fs.id}>
                    <TableCell>{formatDate(fs.reference_date)}</TableCell>
                    <TableCell>
                      <Badge variant="secondary">{fs.statement_type}</Badge>
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {fs.account_code || "\u2014"}
                    </TableCell>
                    <TableCell>{fs.account_name || "\u2014"}</TableCell>
                    <TableCell className="text-right font-mono">
                      {fs.value != null ? formatCurrency(fs.value) : "\u2014"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>

            {statementsTotalPages > 1 && (
              <div className="flex items-center justify-center gap-2 pt-4">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={statementsPage <= 1}
                  onClick={() => setStatementsPage(statementsPage - 1)}
                >
                  Anterior
                </Button>
                <span className="text-sm text-muted-foreground">
                  Página {statementsPage} de {statementsTotalPages}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={statementsPage >= statementsTotalPages}
                  onClick={() => setStatementsPage(statementsPage + 1)}
                >
                  Próxima
                </Button>
              </div>
            )}
          </>
        )}
      </div>

      {/* Dividends Section */}
      <div className="space-y-4">
        <h3 className="text-lg font-semibold">Dividendos</h3>
        <div className="flex flex-col sm:flex-row gap-3">
          <Input
            type="date"
            value={dividendsDateFrom}
            onChange={(e) => {
              setDividendsDateFrom(e.target.value);
              setDividendsPage(1);
            }}
            className="w-full sm:w-[160px]"
          />
          <Input
            type="date"
            value={dividendsDateTo}
            onChange={(e) => {
              setDividendsDateTo(e.target.value);
              setDividendsPage(1);
            }}
            className="w-full sm:w-[160px]"
          />
        </div>

        {dividendsLoading ? (
          <div className="flex items-center justify-center py-12 text-muted-foreground">
            Carregando...
          </div>
        ) : dividends.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
            <DollarSign className="h-12 w-12 mb-3" />
            <p className="text-lg">Nenhum dividendo encontrado</p>
          </div>
        ) : (
          <>
            <div className="text-sm text-muted-foreground">
              {formatQuantity(dividendsTotal)} registro
              {dividendsTotal !== 1 ? "s" : ""}
            </div>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Data Ex</TableHead>
                  <TableHead>Pagamento</TableHead>
                  <TableHead>Tipo</TableHead>
                  <TableHead className="text-right">Valor/Ação</TableHead>
                  <TableHead className="text-right">Valor Total</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {dividends.map((dv) => (
                  <TableRow key={dv.id}>
                    <TableCell>
                      {dv.ex_date ? formatDate(dv.ex_date) : "\u2014"}
                    </TableCell>
                    <TableCell>
                      {dv.payment_date
                        ? formatDate(dv.payment_date)
                        : "\u2014"}
                    </TableCell>
                    <TableCell>
                      {dv.dividend_type ? (
                        <Badge variant="secondary">{dv.dividend_type}</Badge>
                      ) : (
                        "\u2014"
                      )}
                    </TableCell>
                    <TableCell className="text-right font-mono">
                      {dv.value_per_share != null
                        ? formatCurrency(dv.value_per_share)
                        : "\u2014"}
                    </TableCell>
                    <TableCell className="text-right font-mono">
                      {dv.total_value != null
                        ? formatCurrency(dv.total_value)
                        : "\u2014"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>

            {dividendsTotalPages > 1 && (
              <div className="flex items-center justify-center gap-2 pt-4">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={dividendsPage <= 1}
                  onClick={() => setDividendsPage(dividendsPage - 1)}
                >
                  Anterior
                </Button>
                <span className="text-sm text-muted-foreground">
                  Página {dividendsPage} de {dividendsTotalPages}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={dividendsPage >= dividendsTotalPages}
                  onClick={() => setDividendsPage(dividendsPage + 1)}
                >
                  Próxima
                </Button>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

interface CompanyDetailClientProps {
  company: CompanyDetail;
}

export function CompanyDetailClient({ company }: CompanyDetailClientProps) {
  return (
    <div className="container mx-auto px-4 py-8 space-y-6">
      <CompanyHeader company={company} />

      <Tabs defaultValue="posicoes">
        <TabsList>
          <TabsTrigger value="posicoes">Posições Atuais</TabsTrigger>
          <TabsTrigger value="movimentacoes">Movimentações</TabsTrigger>
          <TabsTrigger value="evolucao">Evolução</TabsTrigger>
          <TabsTrigger value="documentos">Documentos</TabsTrigger>
          <TabsTrigger value="financeiro">Financeiro</TabsTrigger>
        </TabsList>

        <TabsContent value="posicoes">
          <PositionsTab positions={company.current_positions} />
        </TabsContent>

        <TabsContent value="movimentacoes">
          <MovementsTab companyId={company.id} />
        </TabsContent>

        <TabsContent value="evolucao">
          <EvolutionTab companyId={company.id} />
        </TabsContent>

        <TabsContent value="documentos">
          <DocumentsTab companyId={company.id} />
        </TabsContent>

        <TabsContent value="financeiro">
          <FinancialTab companyId={company.id} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
