"use client";

import { useState, useEffect, useCallback, use } from "react";
import Link from "next/link";
import { Card, CardContent } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
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
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { api } from "@/lib/api";
import {
  formatCurrency,
  formatDate,
  formatQuantity,
  displayTicker,
} from "@/lib/utils";
import type {
  InsiderSummary,
  Holding,
  InsiderPosition,
  PaginatedResponse,
} from "@/lib/types";

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

function SummaryCards({ summary }: { summary: InsiderSummary }) {
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      <Card>
        <CardContent className="pt-6">
          <div className="text-sm text-muted-foreground">Total Operações</div>
          <div className="text-2xl font-bold">
            {formatQuantity(summary.total_operations)}
          </div>
          <div className="text-xs text-muted-foreground mt-1">
            {formatQuantity(summary.buy_count)} compras /{" "}
            {formatQuantity(summary.sell_count)} vendas
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardContent className="pt-6">
          <div className="text-sm text-muted-foreground">Valor Total</div>
          <div className="text-2xl font-bold">
            {formatCurrency(summary.total_value)}
          </div>
          <div className="text-xs text-muted-foreground mt-1">
            Compras: {formatCurrency(summary.buy_value)}
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardContent className="pt-6">
          <div className="text-sm text-muted-foreground">Empresas</div>
          <div className="text-2xl font-bold">{summary.companies_count}</div>
          <div className="text-xs text-muted-foreground mt-1">
            {summary.insider_group || "Sem grupo"}
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardContent className="pt-6">
          <div className="text-sm text-muted-foreground">Alertas</div>
          <div className="text-2xl font-bold">{summary.alert_count}</div>
          <div className="text-xs text-muted-foreground mt-1">
            {summary.first_operation && summary.last_operation
              ? `${formatDate(summary.first_operation)} - ${formatDate(summary.last_operation)}`
              : "\u2014"}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function CompaniesSection({ summary }: { summary: InsiderSummary }) {
  if (summary.companies.length === 0) return null;

  return (
    <div className="space-y-3">
      <h3 className="text-lg font-semibold">Empresas</h3>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Empresa</TableHead>
            <TableHead>Ticker</TableHead>
            <TableHead className="text-right">Operações</TableHead>
            <TableHead className="text-right">Valor Total</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {summary.companies.map((c) => (
            <TableRow key={c.company_id}>
              <TableCell>
                <Link
                  href={`/empresas/${c.company_id}`}
                  className="font-medium text-primary hover:underline"
                >
                  {c.company_name}
                </Link>
              </TableCell>
              <TableCell className="font-mono">
                {displayTicker(c.company_ticker)}
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatQuantity(c.operations)}
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatCurrency(c.total_value)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function HoldingsTab({ insiderName }: { insiderName: string }) {
  const [data, setData] = useState<Holding[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const perPage = 20;
  const [loading, setLoading] = useState(true);
  const [operationType, setOperationType] = useState("");
  const [assetType, setAssetType] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");

  const fetchHoldings = useCallback(async () => {
    setLoading(true);
    try {
      const params: Record<string, string | number> = {
        page,
        per_page: perPage,
      };
      if (operationType) params.operation_type = operationType;
      if (assetType) params.asset_type = assetType;
      if (dateFrom) params.date_from = dateFrom;
      if (dateTo) params.date_to = dateTo;

      const result = await api.get<PaginatedResponse<Holding>>(
        `/insiders/${encodeURIComponent(insiderName)}/holdings`,
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
  }, [insiderName, page, operationType, assetType, dateFrom, dateTo]);

  useEffect(() => {
    fetchHoldings();
  }, [fetchHoldings]);

  const totalPages = Math.ceil(total / perPage);

  return (
    <div className="space-y-4">
      <div className="flex flex-col sm:flex-row gap-3">
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
            {formatQuantity(total)} movimentaç
            {total !== 1 ? "ões" : "ão"} encontrada
            {total !== 1 ? "s" : ""}
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Data</TableHead>
                <TableHead>Empresa</TableHead>
                <TableHead>Ativo</TableHead>
                <TableHead>Operação</TableHead>
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
                    {h.company_id ? (
                      <Link
                        href={`/empresas/${h.company_id}`}
                        className="text-primary hover:underline"
                      >
                        {h.company_name || "\u2014"}
                      </Link>
                    ) : (
                      h.company_name || "\u2014"
                    )}
                    {h.company_ticker && (
                      <span className="text-xs text-muted-foreground ml-1">
                        ({h.company_ticker})
                      </span>
                    )}
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

function PositionsTab({ insiderName }: { insiderName: string }) {
  const [data, setData] = useState<InsiderPosition[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const perPage = 20;
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      try {
        const result = await api.get<PaginatedResponse<InsiderPosition>>(
          `/insiders/${encodeURIComponent(insiderName)}/positions`,
          { params: { page, per_page: perPage } }
        );
        if (!cancelled) {
          setData(result.data);
          setTotal(result.total);
        }
      } catch {
        if (!cancelled) {
          setData([]);
          setTotal(0);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [insiderName, page]);

  const totalPages = Math.ceil(total / perPage);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-16 text-muted-foreground">
        Carregando...
      </div>
    );
  }

  if (data.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
        <p className="text-lg">Nenhuma posição encontrada</p>
        <p className="text-sm">Este insider não possui registros de posição.</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="text-sm text-muted-foreground">
        {formatQuantity(total)} registro{total !== 1 ? "s" : ""}
      </div>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Data Ref.</TableHead>
            <TableHead>Empresa</TableHead>
            <TableHead>Tipo</TableHead>
            <TableHead>Descrição</TableHead>
            <TableHead className="text-right">Quantidade</TableHead>
            <TableHead className="text-right">Valor Total</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((pos) => (
            <TableRow key={pos.id}>
              <TableCell>{formatDate(pos.reference_date)}</TableCell>
              <TableCell>
                <Link
                  href={`/empresas/${pos.company_id}`}
                  className="text-primary hover:underline"
                >
                  {pos.company_name || "\u2014"}
                </Link>
                {pos.company_ticker && (
                  <span className="text-xs text-muted-foreground ml-1">
                    ({pos.company_ticker})
                  </span>
                )}
              </TableCell>
              <TableCell>{pos.asset_type || "\u2014"}</TableCell>
              <TableCell>{pos.asset_description || "\u2014"}</TableCell>
              <TableCell className="text-right font-mono">
                {pos.quantity != null ? formatQuantity(pos.quantity) : "\u2014"}
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

interface Props {
  params: Promise<{ name: string }>;
}

export default function InsiderDetailPage({ params }: Props) {
  const { name } = use(params);
  const insiderName = decodeURIComponent(name);
  const [summary, setSummary] = useState<InsiderSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      try {
        const result = await api.get<{ data: InsiderSummary }>(
          `/insiders/${encodeURIComponent(insiderName)}/summary`
        );
        if (!cancelled) setSummary(result.data);
      } catch {
        if (!cancelled) setError(true);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [insiderName]);

  if (loading) {
    return (
      <div className="container mx-auto px-4 py-8">
        <div className="flex items-center justify-center py-16 text-muted-foreground">
          Carregando...
        </div>
      </div>
    );
  }

  if (error || !summary) {
    return (
      <div className="container mx-auto px-4 py-8">
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <p className="text-lg">Insider não encontrado</p>
          <Link href="/insiders" className="text-primary hover:underline mt-2">
            Voltar para lista de insiders
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8 space-y-6">
      <div className="space-y-1">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-bold">{summary.insider_name}</h1>
          {summary.insider_group && (
            <Badge variant="secondary">{summary.insider_group}</Badge>
          )}
        </div>
        <p className="text-sm text-muted-foreground">
          Histórico de negociações em {summary.companies_count} empresa
          {summary.companies_count !== 1 ? "s" : ""}
        </p>
      </div>

      <SummaryCards summary={summary} />

      <Tabs defaultValue="movimentacoes">
        <TabsList>
          <TabsTrigger value="movimentacoes">Movimentações</TabsTrigger>
          <TabsTrigger value="empresas">Empresas</TabsTrigger>
          <TabsTrigger value="posicoes">Posições</TabsTrigger>
        </TabsList>

        <TabsContent value="movimentacoes">
          <HoldingsTab insiderName={insiderName} />
        </TabsContent>

        <TabsContent value="empresas">
          <CompaniesSection summary={summary} />
        </TabsContent>

        <TabsContent value="posicoes">
          <PositionsTab insiderName={insiderName} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
