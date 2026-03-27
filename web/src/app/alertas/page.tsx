"use client";

import { useState, useEffect, useCallback } from "react";
import Link from "next/link";
import { Bell, CheckCheck, Eye } from "lucide-react";
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
import { formatQuantity, displayTicker } from "@/lib/utils";
import type {
  PaginatedResponse,
  Alert,
  AlertsSummary,
} from "@/lib/types";

const ALERT_TYPES = [
  { value: "all", label: "Todos os tipos" },
  { value: "alto_valor", label: "Alto Valor" },
  { value: "volume_atipico", label: "Volume Atípico" },
  { value: "mudanca_direcao", label: "Mudança Direção" },
  { value: "retorno_atividade", label: "Retorno Atividade" },
];

const SEVERITIES = [
  { value: "all", label: "Todas" },
  { value: "critical", label: "Crítica" },
  { value: "high", label: "Alta" },
  { value: "medium", label: "Média" },
  { value: "low", label: "Baixa" },
];

const READ_STATUS = [
  { value: "all", label: "Todos" },
  { value: "unread", label: "Não lidos" },
  { value: "read", label: "Lidos" },
];

const ALERT_TYPE_LABELS: Record<string, string> = {
  alto_valor: "Alto Valor",
  volume_atipico: "Volume Atípico",
  mudanca_direcao: "Mudança Direção",
  retorno_atividade: "Retorno Atividade",
};

const SEVERITY_LABELS: Record<string, string> = {
  critical: "Crítica",
  high: "Alta",
  medium: "Média",
  low: "Baixa",
};

const SEVERITY_VARIANT: Record<string, "default" | "secondary" | "outline" | "destructive"> = {
  critical: "destructive",
  high: "default",
  medium: "secondary",
  low: "outline",
};

function formatDateTime(dateStr: string): string {
  const d = new Date(dateStr);
  return d.toLocaleDateString("pt-BR", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export default function AlertsPage() {
  const [alertType, setAlertType] = useState("all");
  const [severity, setSeverity] = useState("all");
  const [readStatus, setReadStatus] = useState("all");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
  const perPage = 20;

  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [total, setTotal] = useState(0);
  const [summary, setSummary] = useState<AlertsSummary | null>(null);
  const [loading, setLoading] = useState(true);

  const buildParams = useCallback(() => {
    const params: Record<string, string | number | boolean> = {};
    if (alertType !== "all") params.alert_type = alertType;
    if (severity !== "all") params.severity = severity;
    if (readStatus === "unread") params.is_read = false;
    if (readStatus === "read") params.is_read = true;
    if (dateFrom) params.date_from = dateFrom;
    if (dateTo) params.date_to = dateTo;
    if (search) params.search = search;
    return params;
  }, [alertType, severity, readStatus, dateFrom, dateTo, search]);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = buildParams();

      const [alertsRes, summaryRes] = await Promise.allSettled([
        api.get<PaginatedResponse<Alert>>("/alerts", {
          params: { ...params, page, per_page: perPage },
        }),
        api.get<{ data: AlertsSummary }>("/alerts/summary"),
      ]);

      if (alertsRes.status === "fulfilled") {
        setAlerts(alertsRes.value.data);
        setTotal(alertsRes.value.total);
      } else {
        setAlerts([]);
        setTotal(0);
      }

      setSummary(
        summaryRes.status === "fulfilled" ? summaryRes.value.data : null
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
  }, [alertType, severity, readStatus, dateFrom, dateTo, search]);

  const handleMarkRead = async (alertId: number) => {
    await api.patch(`/alerts/${alertId}/read`);
    fetchData();
  };

  const handleMarkAllRead = async () => {
    const params: Record<string, string> = {};
    if (alertType !== "all") params.alert_type = alertType;
    await api.patch("/alerts/mark-all-read", { params });
    fetchData();
  };

  const totalPages = Math.ceil(total / perPage);

  return (
    <div className="container mx-auto px-4 py-8 space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div className="flex items-center gap-2">
          <Bell className="h-6 w-6 text-primary" />
          <h1 className="text-2xl font-bold">Alertas</h1>
          {summary && summary.unread > 0 && (
            <Badge variant="destructive">{summary.unread} não lidos</Badge>
          )}
        </div>

        <div className="flex gap-3 flex-wrap items-center">
          <Select value={alertType} onValueChange={setAlertType}>
            <SelectTrigger className="w-[180px]">
              <SelectValue placeholder="Tipo" />
            </SelectTrigger>
            <SelectContent>
              {ALERT_TYPES.map((t) => (
                <SelectItem key={t.value} value={t.value}>
                  {t.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Select value={severity} onValueChange={setSeverity}>
            <SelectTrigger className="w-[140px]">
              <SelectValue placeholder="Severidade" />
            </SelectTrigger>
            <SelectContent>
              {SEVERITIES.map((s) => (
                <SelectItem key={s.value} value={s.value}>
                  {s.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Select value={readStatus} onValueChange={setReadStatus}>
            <SelectTrigger className="w-[140px]">
              <SelectValue placeholder="Status" />
            </SelectTrigger>
            <SelectContent>
              {READ_STATUS.map((r) => (
                <SelectItem key={r.value} value={r.value}>
                  {r.label}
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

          <Input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-[200px]"
            placeholder="Buscar..."
          />
        </div>
      </div>

      {/* Summary cards */}
      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Total de Alertas
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {formatQuantity(summary.total)}
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Não Lidos
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-destructive">
                {formatQuantity(summary.unread)}
              </div>
            </CardContent>
          </Card>
          {summary.by_severity
            .filter((s) => s.severity === "critical" || s.severity === "high")
            .map((s) => (
              <Card key={s.severity}>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground">
                    {SEVERITY_LABELS[s.severity] || s.severity}
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-2xl font-bold">
                    {formatQuantity(s.count)}
                  </div>
                </CardContent>
              </Card>
            ))}
        </div>
      )}

      {/* Alerts table */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-lg">Lista de Alertas</CardTitle>
          {summary && summary.unread > 0 && (
            <Button
              variant="outline"
              size="sm"
              onClick={handleMarkAllRead}
              className="flex items-center gap-1"
            >
              <CheckCheck className="h-4 w-4" />
              Marcar todos como lidos
            </Button>
          )}
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="flex items-center justify-center py-16 text-muted-foreground">
              Carregando...
            </div>
          ) : alerts.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-16 text-muted-foreground space-y-2">
              <p className="text-lg">Nenhum alerta encontrado</p>
              <p className="text-sm">
                Ajuste os filtros ou aguarde novas sincronizações.
              </p>
            </div>
          ) : (
            <>
              <div className="border rounded-md overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-[40px]"></TableHead>
                      <TableHead>Empresa</TableHead>
                      <TableHead>Título</TableHead>
                      <TableHead>Tipo</TableHead>
                      <TableHead>Severidade</TableHead>
                      <TableHead>Data</TableHead>
                      <TableHead className="w-[60px]"></TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {alerts.map((a) => (
                      <TableRow
                        key={a.id}
                        className={a.is_read ? "opacity-60" : ""}
                      >
                        <TableCell>
                          {!a.is_read && (
                            <span className="inline-block w-2 h-2 rounded-full bg-primary" />
                          )}
                        </TableCell>
                        <TableCell>
                          <Link
                            href={`/empresas/${a.company_id}`}
                            className="text-primary hover:underline font-medium"
                          >
                            {displayTicker(a.company_ticker)}
                          </Link>
                          {a.company_name && (
                            <span className="block text-xs text-muted-foreground">
                              {a.company_name}
                            </span>
                          )}
                        </TableCell>
                        <TableCell className="max-w-[300px]">
                          <span className="font-medium">{a.title}</span>
                          {a.description && (
                            <span
                              className="block text-xs text-muted-foreground truncate max-w-[300px]"
                              title={a.description}
                            >
                              {a.description}
                            </span>
                          )}
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline">
                            {ALERT_TYPE_LABELS[a.alert_type] || a.alert_type}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <Badge variant={SEVERITY_VARIANT[a.severity] || "secondary"}>
                            {SEVERITY_LABELS[a.severity] || a.severity}
                          </Badge>
                        </TableCell>
                        <TableCell className="font-mono text-sm whitespace-nowrap">
                          {formatDateTime(a.created_at)}
                        </TableCell>
                        <TableCell>
                          {!a.is_read && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleMarkRead(a.id)}
                              title="Marcar como lido"
                            >
                              <Eye className="h-4 w-4" />
                            </Button>
                          )}
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
