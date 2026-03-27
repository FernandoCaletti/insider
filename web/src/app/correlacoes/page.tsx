"use client";

import { useState, useEffect, useCallback } from "react";
import Link from "next/link";
import { GitCompareArrows, ArrowDown, ArrowUp } from "lucide-react";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { api } from "@/lib/api";
import {
  formatCurrency,
  formatDate,
  formatQuantity,
  displayTicker,
} from "@/lib/utils";
import type {
  PaginatedResponse,
  CorrelationEntry,
  CorrelationSummary,
  TopCorrelatedCompany,
} from "@/lib/types";

const WINDOW_OPTIONS = [
  { value: "7", label: "7 dias" },
  { value: "15", label: "15 dias" },
  { value: "30", label: "30 dias" },
  { value: "60", label: "60 dias" },
  { value: "90", label: "90 dias" },
];

const OPERATION_TYPES = [
  { value: "all", label: "Todas" },
  { value: "Compra", label: "Compra" },
  { value: "Venda", label: "Venda" },
];

export default function CorrelationsPage() {
  const [daysWindow, setDaysWindow] = useState("30");
  const [operationType, setOperationType] = useState("all");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [page, setPage] = useState(1);
  const perPage = 50;

  const [correlations, setCorrelations] = useState<CorrelationEntry[]>([]);
  const [total, setTotal] = useState(0);
  const [summary, setSummary] = useState<CorrelationSummary | null>(null);
  const [topCompanies, setTopCompanies] = useState<TopCorrelatedCompany[]>([]);
  const [loading, setLoading] = useState(true);

  const buildParams = useCallback(() => {
    const params: Record<string, string | number> = {
      days_window: parseInt(daysWindow),
    };
    if (operationType !== "all") params.operation_type = operationType;
    if (dateFrom) params.date_from = dateFrom;
    if (dateTo) params.date_to = dateTo;
    return params;
  }, [daysWindow, operationType, dateFrom, dateTo]);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = buildParams();

      const [correlRes, summaryRes, topRes] = await Promise.allSettled([
        api.get<PaginatedResponse<CorrelationEntry>>("/correlations", {
          params: { ...params, page, per_page: perPage },
        }),
        api.get<{ data: CorrelationSummary }>("/correlations/summary", {
          params,
        }),
        api.get<{ data: TopCorrelatedCompany[] }>("/correlations/top-companies", {
          params: { ...params, limit: 10 },
        }),
      ]);

      if (correlRes.status === "fulfilled") {
        setCorrelations(correlRes.value.data);
        setTotal(correlRes.value.total);
      } else {
        setCorrelations([]);
        setTotal(0);
      }

      setSummary(
        summaryRes.status === "fulfilled" ? summaryRes.value.data : null
      );
      setTopCompanies(
        topRes.status === "fulfilled" ? topRes.value.data : []
      );
    } finally {
      setLoading(false);
    }
  }, [buildParams, page, perPage]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Reset page when filters change
  useEffect(() => {
    setPage(1);
  }, [daysWindow, operationType, dateFrom, dateTo]);

  const totalPages = Math.ceil(total / perPage);

  return (
    <div className="container mx-auto px-4 py-8 space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div className="flex items-center gap-2">
          <GitCompareArrows className="h-6 w-6 text-primary" />
          <h1 className="text-2xl font-bold">Correlações</h1>
        </div>

        <div className="flex gap-3 flex-wrap items-center">
          <Select value={daysWindow} onValueChange={setDaysWindow}>
            <SelectTrigger className="w-[150px]">
              <SelectValue placeholder="Janela" />
            </SelectTrigger>
            <SelectContent>
              {WINDOW_OPTIONS.map((w) => (
                <SelectItem key={w.value} value={w.value}>
                  {w.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Select value={operationType} onValueChange={setOperationType}>
            <SelectTrigger className="w-[150px]">
              <SelectValue placeholder="Operação" />
            </SelectTrigger>
            <SelectContent>
              {OPERATION_TYPES.map((o) => (
                <SelectItem key={o.value} value={o.value}>
                  {o.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Input
            type="date"
            value={dateFrom}
            onChange={(e) => setDateFrom(e.target.value)}
            className="w-[160px]"
            placeholder="De"
          />
          <Input
            type="date"
            value={dateTo}
            onChange={(e) => setDateTo(e.target.value)}
            className="w-[160px]"
            placeholder="Até"
          />
        </div>
      </div>

      {/* Summary cards */}
      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Correlações
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {formatQuantity(summary.total_correlations)}
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Empresas
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {formatQuantity(summary.companies_involved)}
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Antes do Fato
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold flex items-center gap-1">
                <ArrowDown className="h-5 w-5 text-orange-500" />
                {formatQuantity(summary.movements_before_fact)}
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Após o Fato
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold flex items-center gap-1">
                <ArrowUp className="h-5 w-5 text-blue-500" />
                {formatQuantity(summary.movements_after_fact)}
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Top companies */}
      {topCompanies.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">
              Empresas com Mais Correlações
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="border rounded-md overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-[50px]">#</TableHead>
                    <TableHead>Empresa</TableHead>
                    <TableHead>Ticker</TableHead>
                    <TableHead className="text-right">Correlações</TableHead>
                    <TableHead className="text-right">Movimentações</TableHead>
                    <TableHead className="text-right">Fatos</TableHead>
                    <TableHead className="text-right">Valor Total</TableHead>
                    <TableHead className="text-right">Dias (média)</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {topCompanies.map((c, i) => (
                    <TableRow key={c.company_id}>
                      <TableCell className="font-medium text-muted-foreground">
                        {i + 1}
                      </TableCell>
                      <TableCell>
                        <Link
                          href={`/empresas/${c.company_id}`}
                          className="text-primary hover:underline font-medium"
                        >
                          {c.company_name}
                        </Link>
                      </TableCell>
                      <TableCell className="font-mono text-sm">
                        {displayTicker(c.company_ticker)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {formatQuantity(c.correlation_count)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {formatQuantity(c.unique_movements)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {formatQuantity(c.unique_facts)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {formatCurrency(c.total_value)}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {c.avg_days_diff}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Correlation list */}
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">
            Movimentações Próximas a Fatos Relevantes
          </CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center justify-center py-16 text-muted-foreground">
              Carregando...
            </div>
          ) : correlations.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-16 text-muted-foreground space-y-2">
              <p className="text-lg">Nenhuma correlação encontrada</p>
              <p className="text-sm">
                Tente aumentar a janela de dias ou ajustar os filtros.
              </p>
            </div>
          ) : (
            <>
              <div className="border rounded-md overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Empresa</TableHead>
                      <TableHead>Operação</TableHead>
                      <TableHead>Tipo Ativo</TableHead>
                      <TableHead className="text-right">Valor</TableHead>
                      <TableHead>Data Movimento</TableHead>
                      <TableHead>Data Fato</TableHead>
                      <TableHead className="text-right">Dias</TableHead>
                      <TableHead>Grupo</TableHead>
                      <TableHead className="min-w-[200px]">Assunto</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {correlations.map((c) => (
                      <TableRow
                        key={`${c.holding_id}-${c.material_fact_id}`}
                      >
                        <TableCell>
                          <Link
                            href={`/empresas/${c.company_id}`}
                            className="text-primary hover:underline font-medium"
                          >
                            {displayTicker(c.company_ticker)}
                          </Link>
                        </TableCell>
                        <TableCell>
                          {c.operation_type ? (
                            <Badge
                              variant={
                                c.operation_type === "Compra"
                                  ? "default"
                                  : "secondary"
                              }
                            >
                              {c.operation_type}
                            </Badge>
                          ) : (
                            "\u2014"
                          )}
                        </TableCell>
                        <TableCell className="text-sm">
                          {c.asset_type}
                        </TableCell>
                        <TableCell className="text-right font-mono">
                          {c.total_value != null
                            ? formatCurrency(c.total_value)
                            : "\u2014"}
                        </TableCell>
                        <TableCell className="font-mono text-sm">
                          {formatDate(c.operation_date)}
                        </TableCell>
                        <TableCell className="font-mono text-sm">
                          {formatDate(c.fact_date)}
                        </TableCell>
                        <TableCell className="text-right">
                          <Badge
                            variant={
                              c.days_diff < 0 ? "outline" : "secondary"
                            }
                          >
                            {c.days_diff > 0
                              ? `+${c.days_diff}`
                              : c.days_diff}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-sm">
                          {c.insider_group || "\u2014"}
                        </TableCell>
                        <TableCell
                          className="text-sm text-muted-foreground max-w-[300px] truncate"
                          title={c.fact_subject || ""}
                        >
                          {c.fact_subject || "\u2014"}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="flex items-center justify-between mt-4">
                  <p className="text-sm text-muted-foreground">
                    {formatQuantity(total)} resultados
                  </p>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={page <= 1}
                      onClick={() => setPage(page - 1)}
                    >
                      Anterior
                    </Button>
                    <span className="flex items-center text-sm px-2">
                      {page} / {totalPages}
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
                </div>
              )}
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
