"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import type { MovementChartPoint } from "@/lib/types";

function formatShortDate(dateStr: string): string {
  const d = new Date(dateStr + "T00:00:00");
  return d.toLocaleDateString("pt-BR", { day: "2-digit", month: "2-digit" });
}

function formatCurrencyShort(value: number): string {
  if (Math.abs(value) >= 1_000_000) {
    return `R$ ${(value / 1_000_000).toFixed(1)}M`;
  }
  if (Math.abs(value) >= 1_000) {
    return `R$ ${(value / 1_000).toFixed(0)}K`;
  }
  return `R$ ${value.toFixed(0)}`;
}

function CustomTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: Array<{ dataKey?: string | number; name?: string; value?: number; color?: string }>;
  label?: string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-lg border bg-card p-3 shadow-md">
      <p className="text-sm font-medium mb-1">{label}</p>
      {payload.map((entry) => (
        <p key={entry.dataKey} className="text-sm" style={{ color: entry.color }}>
          {entry.name}: {entry.value?.toLocaleString("pt-BR")}
        </p>
      ))}
    </div>
  );
}

export function MovementsChart({ data }: { data: MovementChartPoint[] }) {
  const chartData = data.map((point) => ({
    ...point,
    date: formatShortDate(point.date),
  }));

  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={chartData}>
        <CartesianGrid strokeDasharray="3 3" vertical={false} />
        <XAxis dataKey="date" fontSize={12} tickLine={false} axisLine={false} />
        <YAxis
          fontSize={12}
          tickLine={false}
          axisLine={false}
          tickFormatter={formatCurrencyShort}
        />
        <Tooltip content={<CustomTooltip />} />
        <Bar
          dataKey="valor_compras"
          name="Compras"
          fill="#22C55E"
          radius={[4, 4, 0, 0]}
        />
        <Bar
          dataKey="valor_vendas"
          name="Vendas"
          fill="#EF4444"
          radius={[4, 4, 0, 0]}
        />
      </BarChart>
    </ResponsiveContainer>
  );
}
