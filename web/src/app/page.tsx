import Link from "next/link";
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
import { HeroInsight } from "@/components/dashboard/hero-insight";
import { MarketTemperature } from "@/components/dashboard/market-temperature";
import { ActivityRadar } from "@/components/dashboard/activity-radar";
import { SummaryCards } from "@/components/dashboard/summary-cards";
import type { DashboardSummaryExtended } from "@/components/dashboard/summary-cards";
import { MovementsChart } from "@/components/dashboard/movements-chart";
import { api } from "@/lib/api";
import {
  formatCurrency,
  formatDate,
  formatQuantity,
  displayTicker,
} from "@/lib/utils";
import type { RecentMovement, SyncStatus } from "@/lib/types";

export const revalidate = 3600;

interface DashboardSummaryResponse {
  total_companies: number;
  total_documents: number;
  total_movements: number;
  new_companies_this_month?: number;
  last_sync_docs?: number;
  movements_30d?: number;
  movements_30d_change_pct?: number;
  balance_30d?: number;
  balance_previous_30d?: number;
  balance_change_pct?: number;
  last_sync: SyncStatus | null;
  data_range: { from: string | null; to: string | null };
}

async function getDashboardData() {
  const [summary, movements] = await Promise.allSettled([
    api.get<{ data: DashboardSummaryResponse }>("/dashboard/summary", {
      next: { revalidate: 3600 },
    }),
    api.get<{ data: RecentMovement[] }>("/dashboard/recent-movements", {
      params: { days: 90, limit: 10 },
      next: { revalidate: 3600 },
    }),
  ]);

  const summaryData =
    summary.status === "fulfilled"
      ? summary.value.data
      : {
          total_companies: 0,
          total_documents: 0,
          total_movements: 0,
          last_sync: null,
          data_range: { from: null, to: null },
        };

  return {
    summary: summaryData,
    summaryExtended: {
      total_companies: summaryData.total_companies,
      total_documents: summaryData.total_documents,
      total_movements: summaryData.total_movements,
      new_companies_this_month: summaryData.new_companies_this_month ?? 0,
      last_sync_docs: summaryData.last_sync_docs ?? 0,
      movements_30d: summaryData.movements_30d ?? 0,
      movements_30d_change_pct: summaryData.movements_30d_change_pct ?? 0,
      balance_30d: summaryData.balance_30d ?? 0,
      balance_30d_change_pct: summaryData.balance_change_pct ?? 0,
    } satisfies DashboardSummaryExtended,
    movements: movements.status === "fulfilled" ? movements.value.data : [],
  };
}

function getOperationVariant(opType: string | null): "success" | "destructive" | "secondary" {
  if (!opType) return "secondary";
  const lower = opType.toLowerCase();
  if (lower.startsWith("compra")) return "success";
  if (lower.startsWith("venda")) return "destructive";
  return "secondary";
}

export default async function DashboardPage() {
  const { summaryExtended, movements } = await getDashboardData();

  return (
    <div className="container mx-auto px-4 py-8 space-y-8">
      {/* BLOCO 1 — Hero Insight + Termômetro */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2">
          <HeroInsight />
        </div>
        <div>
          <MarketTemperature />
        </div>
      </div>

      {/* BLOCO 2 — Radar de Atividade */}
      <ActivityRadar />

      {/* BLOCO 3 — Visão Geral */}
      <SummaryCards data={summaryExtended} />

      {/* Gráfico */}
      <Card>
        <CardHeader>
          <CardTitle>Compras vs Vendas</CardTitle>
        </CardHeader>
        <CardContent>
          <MovementsChart />
        </CardContent>
      </Card>

      {/* Movimentações Recentes */}
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
                  <TableHead>Operação</TableHead>
                  <TableHead>Ativo</TableHead>
                  <TableHead>Grupo</TableHead>
                  <TableHead className="text-right">Quantidade</TableHead>
                  <TableHead className="text-right">Valor</TableHead>
                  <TableHead>Data</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {movements.map((m) => (
                  <TableRow key={m.id}>
                    <TableCell>
                      <Link
                        href={`/empresas/${m.company_id}`}
                        className="text-primary hover:underline font-medium"
                      >
                        {m.company_name}
                      </Link>
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {displayTicker(m.ticker ?? m.company_ticker)}
                    </TableCell>
                    <TableCell>
                      <Badge variant={getOperationVariant(m.operation_type)}>
                        {m.operation_type ?? "\u2014"}
                      </Badge>
                    </TableCell>
                    <TableCell>{m.asset_type}</TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {m.insider_group ?? "\u2014"}
                    </TableCell>
                    <TableCell className="text-right font-mono">
                      {formatQuantity(m.quantity)}
                    </TableCell>
                    <TableCell className="text-right font-mono">
                      {m.total_value != null
                        ? formatCurrency(Math.abs(Number(m.total_value)))
                        : "\u2014"}
                    </TableCell>
                    <TableCell>
                      {m.operation_date
                        ? formatDate(m.operation_date)
                        : "\u2014"}
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
