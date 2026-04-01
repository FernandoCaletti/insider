"use client";

import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
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
  payload?: Array<{ value?: number }>;
  label?: string;
}) {
  if (!active || !payload?.length) return null;
  const posicao = payload[0]?.value ?? 0;
  const variacao = payload[1]?.value ?? 0;

  return (
    <div className="rounded-lg border bg-card p-3 shadow-md space-y-1">
      <p className="text-sm font-medium">{label}</p>
      <p className="text-sm">
        Posição: <span className="font-mono font-bold">{posicao.toLocaleString("pt-BR")}</span> ações
      </p>
      {variacao !== 0 && (
        <p className={`text-sm ${variacao > 0 ? "text-success" : "text-destructive"}`}>
          Variação: <span className="font-mono font-bold">{variacao > 0 ? "+" : ""}{variacao.toLocaleString("pt-BR")}</span> ações
        </p>
      )}
    </div>
  );
}

export function PositionChart({ data }: { data: PositionHistoryPoint[] }) {
  const chartData = data.map((point, i) => {
    const posicao = point.posicao_final;
    const anterior = i > 0 ? data[i - 1].posicao_final : point.posicao_inicial;
    const variacao = posicao - anterior;
    return {
      month: formatMonth(point.month),
      posicao,
      variacao,
    };
  });

  return (
    <ResponsiveContainer width="100%" height={350}>
      <AreaChart data={chartData}>
        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--border)" />
        <XAxis
          dataKey="month"
          fontSize={12}
          tickLine={false}
          axisLine={false}
          stroke="var(--muted-foreground)"
        />
        <YAxis
          fontSize={12}
          tickLine={false}
          axisLine={false}
          tickFormatter={formatQuantityShort}
          stroke="var(--muted-foreground)"
        />
        <Tooltip content={<CustomTooltip />} />
        <Area
          type="monotone"
          dataKey="posicao"
          name="Posição dos Insiders"
          stroke="var(--primary)"
          fill="var(--primary)"
          fillOpacity={0.15}
          strokeWidth={2}
          dot={{ r: 3, fill: "var(--primary)" }}
        />
        <Area
          type="monotone"
          dataKey="variacao"
          name="Variação mensal"
          stroke="transparent"
          fill="transparent"
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
