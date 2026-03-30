"use client";

import { useState, useEffect, useCallback } from "react";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { api } from "@/lib/api";
import { formatCurrency } from "@/lib/utils";
import type { MovementChartPoint } from "@/lib/types";

type Period = "30d" | "90d" | "12m";

const PERIOD_DAYS: Record<Period, number> = {
  "30d": 30,
  "90d": 90,
  "12m": 365,
};

function formatShortDate(dateStr: string): string {
  const d = new Date(dateStr + "T00:00:00");
  return d.toLocaleDateString("pt-BR", { day: "2-digit", month: "2-digit" });
}

function formatCompactCurrency(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1e9) return `R$ ${(value / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `R$ ${(value / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `R$ ${(value / 1e3).toFixed(0)}K`;
  return `R$ ${value.toFixed(0)}`;
}

function CustomTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: Array<{
    dataKey?: string | number;
    name?: string;
    value?: number;
    color?: string;
  }>;
  label?: string;
}) {
  if (!active || !payload?.length) return null;

  const compras = payload.find((p) => p.dataKey === "valor_compras");
  const vendas = payload.find((p) => p.dataKey === "valor_vendas");
  const comprasVal = compras?.value ?? 0;
  const vendasVal = vendas?.value ?? 0;
  const saldo = comprasVal - vendasVal;

  return (
    <div className="rounded-lg border bg-card p-3 shadow-md text-sm space-y-1">
      <p className="font-medium">{label}</p>
      <p className="text-success">
        Compras: {formatCurrency(comprasVal)}
      </p>
      <p className="text-destructive">
        Vendas: {formatCurrency(vendasVal)}
      </p>
      <p className={saldo >= 0 ? "text-success" : "text-destructive"}>
        Saldo: {formatCurrency(saldo)}
      </p>
    </div>
  );
}

export function MovementsChart() {
  const [period, setPeriod] = useState<Period>("90d");
  const [data, setData] = useState<MovementChartPoint[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback((p: Period) => {
    setLoading(true);
    api
      .get<{ data: MovementChartPoint[] }>("/dashboard/movements-chart", {
        params: { days: PERIOD_DAYS[p] },
      })
      .then((res) => setData(res.data))
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    fetchData(period);
  }, [period, fetchData]);

  const chartData = data.map((point) => ({
    ...point,
    date: formatShortDate(point.date),
  }));

  return (
    <Card className="lg:col-span-2">
      <CardHeader className="flex flex-row items-center justify-between">
        <CardTitle>Movimentações</CardTitle>
        <Tabs
          value={period}
          onValueChange={(v) => setPeriod(v as Period)}
        >
          <TabsList>
            <TabsTrigger value="30d">30d</TabsTrigger>
            <TabsTrigger value="90d">90d</TabsTrigger>
            <TabsTrigger value="12m">12m</TabsTrigger>
          </TabsList>
        </Tabs>
      </CardHeader>
      <CardContent>
        {loading ? (
          <div className="flex items-center justify-center h-[300px]">
            <div className="animate-pulse h-full w-full rounded bg-muted" />
          </div>
        ) : chartData.length === 0 ? (
          <div className="flex items-center justify-center h-[300px] text-muted-foreground">
            Sem dados para o período
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis
                dataKey="date"
                fontSize={12}
                tickLine={false}
                axisLine={false}
              />
              <YAxis
                fontSize={12}
                tickLine={false}
                axisLine={false}
                tickFormatter={formatCompactCurrency}
              />
              <Tooltip content={<CustomTooltip />} />
              <Area
                type="monotone"
                dataKey="valor_compras"
                name="Compras"
                stroke="#22C55E"
                fill="#22C55E"
                fillOpacity={0.3}
                strokeWidth={2}
              />
              <Area
                type="monotone"
                dataKey="valor_vendas"
                name="Vendas"
                stroke="#EF4444"
                fill="#EF4444"
                fillOpacity={0.3}
                strokeWidth={2}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}
