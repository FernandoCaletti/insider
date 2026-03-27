"use client";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import type { PositionHistoryPoint } from "@/lib/types";

function formatMonth(month: string): string {
  const [year, m] = month.split("-");
  return `${m}/${year.slice(2)}`;
}

function formatQuantityShort(value: number): string {
  if (Math.abs(value) >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)}M`;
  }
  if (Math.abs(value) >= 1_000) {
    return `${(value / 1_000).toFixed(0)}K`;
  }
  return value.toFixed(0);
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

export function PositionChart({ data }: { data: PositionHistoryPoint[] }) {
  const chartData = data.map((point) => ({
    ...point,
    month: formatMonth(point.month),
  }));

  return (
    <ResponsiveContainer width="100%" height={350}>
      <LineChart data={chartData}>
        <CartesianGrid strokeDasharray="3 3" vertical={false} />
        <XAxis dataKey="month" fontSize={12} tickLine={false} axisLine={false} />
        <YAxis
          fontSize={12}
          tickLine={false}
          axisLine={false}
          tickFormatter={formatQuantityShort}
        />
        <Tooltip content={<CustomTooltip />} />
        <Legend />
        <Line
          type="monotone"
          dataKey="posicao_inicial"
          name="Posição Inicial"
          stroke="#1e40af"
          strokeWidth={2}
          dot={{ r: 3 }}
        />
        <Line
          type="monotone"
          dataKey="posicao_final"
          name="Posição Final"
          stroke="#22C55E"
          strokeWidth={2}
          dot={{ r: 3 }}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
