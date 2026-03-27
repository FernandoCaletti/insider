import Link from "next/link";
import { Building2, FileText, ArrowLeftRight, Clock } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { MovementsChart } from "@/components/dashboard/movements-chart";
import { api } from "@/lib/api";
import {
  formatCurrency,
  formatDate,
  formatQuantity,
  displayTicker,
} from "@/lib/utils";
import type {
  DashboardSummary,
  RecentMovement,
  MovementChartPoint,
  SyncStatus,
} from "@/lib/types";

export const revalidate = 3600;

async function getDashboardData() {
  const [summary, movements, chart, syncStatus] = await Promise.allSettled([
    api.get<{ data: DashboardSummary }>("/dashboard/summary", {
      next: { revalidate: 3600 },
    }),
    api.get<{ data: RecentMovement[] }>("/dashboard/recent-movements", {
      params: { days: 90, limit: 10 },
      next: { revalidate: 3600 },
    }),
    api.get<{ data: MovementChartPoint[] }>("/dashboard/movements-chart", {
      params: { days: 90 },
      next: { revalidate: 3600 },
    }),
    api.get<{ data: SyncStatus | null }>("/sync/status", {
      next: { revalidate: 3600 },
    }),
  ]);

  return {
    summary:
      summary.status === "fulfilled"
        ? summary.value.data
        : {
            total_companies: 0,
            total_documents: 0,
            total_movements: 0,
            last_sync: null,
            data_range: { min_date: null, max_date: null },
          },
    movements: movements.status === "fulfilled" ? movements.value.data : [],
    chart: chart.status === "fulfilled" ? chart.value.data : [],
    syncStatus: syncStatus.status === "fulfilled" ? syncStatus.value.data : null,
  };
}

function SyncStatusIndicator({ sync }: { sync: SyncStatus | null }) {
  if (!sync) {
    return (
      <div className="flex items-center gap-2">
        <span className="h-3 w-3 rounded-full bg-muted-foreground" />
        <span className="text-sm text-muted-foreground">Sem dados</span>
      </div>
    );
  }

  const statusConfig = {
    success: {
      color: "bg-success",
      label: "Operacional",
    },
    running: {
      color: "bg-warning",
      label: "Em execução",
    },
    error: {
      color: "bg-destructive",
      label: "Erro",
    },
  } as const;

  const config = statusConfig[sync.status];

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2">
        <span className={`h-3 w-3 rounded-full ${config.color}`} />
        <span className="text-sm font-medium">{config.label}</span>
      </div>
      <div className="grid grid-cols-2 gap-4 text-sm">
        <div>
          <p className="text-muted-foreground">Última execução</p>
          <p className="font-medium">
            {sync.finished_at
              ? formatDate(sync.finished_at.split("T")[0])
              : formatDate(sync.started_at.split("T")[0])}
          </p>
        </div>
        <div>
          <p className="text-muted-foreground">Documentos novos</p>
          <p className="font-medium">
            {formatQuantity(sync.documents_processed)}
          </p>
        </div>
      </div>
    </div>
  );
}

export default async function DashboardPage() {
  const { summary, movements, chart, syncStatus } =
    await getDashboardData();

  const summaryCards = [
    {
      title: "Empresas monitoradas",
      value: formatQuantity(summary.total_companies),
      icon: Building2,
    },
    {
      title: "Documentos processados",
      value: formatQuantity(summary.total_documents),
      icon: FileText,
    },
    {
      title: "Movimentações registradas",
      value: formatQuantity(summary.total_movements),
      icon: ArrowLeftRight,
    },
    {
      title: "Última atualização",
      value: summary.last_sync?.finished_at
        ? formatDate(summary.last_sync.finished_at.split("T")[0])
        : "\u2014",
      icon: Clock,
    },
  ];

  return (
    <div className="container mx-auto px-4 py-8 space-y-8">
      {/* Summary Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {summaryCards.map((card) => (
          <Card key={card.title}>
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                {card.title}
              </CardTitle>
              <card.icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-bold">{card.value}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Chart and Pipeline Status */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Movimentações - Últimos 90 dias</CardTitle>
          </CardHeader>
          <CardContent>
            {chart.length > 0 ? (
              <MovementsChart data={chart} />
            ) : (
              <div className="flex items-center justify-center h-[300px] text-muted-foreground">
                Sem dados para o periodo
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Status do Pipeline</CardTitle>
          </CardHeader>
          <CardContent>
            <SyncStatusIndicator sync={syncStatus} />
          </CardContent>
        </Card>
      </div>

      {/* Recent Movements Table */}
      <Card>
        <CardHeader>
          <CardTitle>Movimentações recentes</CardTitle>
        </CardHeader>
        <CardContent>
          {movements.length > 0 ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Empresa</TableHead>
                  <TableHead>Ticker</TableHead>
                  <TableHead>Tipo</TableHead>
                  <TableHead>Ativo</TableHead>
                  <TableHead className="text-right">Quantidade</TableHead>
                  <TableHead className="text-right">Valor</TableHead>
                  <TableHead>Data</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {movements.map((movement) => (
                  <TableRow key={movement.id}>
                    <TableCell>
                      <Link
                        href={`/empresas/${movement.company_id}`}
                        className="text-primary hover:underline font-medium"
                      >
                        {movement.company_name}
                      </Link>
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {displayTicker(movement.company_ticker)}
                    </TableCell>
                    <TableCell>
                      <Badge
                        variant={
                          movement.operation_type === "Compra"
                            ? "success"
                            : "destructive"
                        }
                      >
                        {movement.operation_type}
                      </Badge>
                    </TableCell>
                    <TableCell>{movement.asset_type}</TableCell>
                    <TableCell className="text-right font-mono">
                      {formatQuantity(movement.quantity)}
                    </TableCell>
                    <TableCell className="text-right font-mono">
                      {formatCurrency(Math.abs(movement.total_value))}
                    </TableCell>
                    <TableCell>
                      {formatDate(movement.operation_date)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <p className="text-center text-muted-foreground py-8">
              Nenhuma movimentação encontrada
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
