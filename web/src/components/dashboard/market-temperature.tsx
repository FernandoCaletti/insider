"use client";

import { useState, useEffect } from "react";
import { ArrowUp, ArrowDown } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { api } from "@/lib/api";
import { formatCurrency } from "@/lib/utils";

interface MarketTemperatureData {
  total_buys: number;
  total_sells: number;
  balance: number;
  ratio: number;
  label: string;
  sentiment: string;
  operations_count: { buys: number; sells: number };
  vs_previous_period: {
    buys_change_pct: number;
    sells_change_pct: number;
    balance_change_pct: number;
  };
}

function formatCompactCurrency(value: number): string {
  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  if (abs >= 1e9) return `${sign}R$ ${(abs / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${sign}R$ ${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}R$ ${(abs / 1e3).toFixed(1)}K`;
  return formatCurrency(value);
}

function getSentimentColor(sentiment: string): string {
  switch (sentiment) {
    case "positive":
    case "bullish":
    case "buying":
      return "text-success";
    case "negative":
    case "bearish":
    case "selling":
      return "text-destructive";
    default:
      return "text-muted-foreground";
  }
}

export function MarketTemperature() {
  const [data, setData] = useState<MarketTemperatureData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    api
      .get<{ data: MarketTemperatureData }>("/dashboard/market-temperature")
      .then((res) => setData(res.data))
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Termômetro do Mercado</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="animate-pulse space-y-4">
            <div className="h-3 w-full rounded-full bg-muted" />
            <div className="h-5 w-32 rounded bg-muted mx-auto" />
            <div className="grid grid-cols-3 gap-4">
              <div className="h-12 rounded bg-muted" />
              <div className="h-12 rounded bg-muted" />
              <div className="h-12 rounded bg-muted" />
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error || !data) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Termômetro do Mercado</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-center text-muted-foreground py-4">
            Dados indisponíveis no momento
          </p>
        </CardContent>
      </Card>
    );
  }

  const indicatorPosition = Math.max(0, Math.min(1, data.ratio)) * 100;
  const balanceChangePct = data.vs_previous_period?.balance_change_pct ?? 0;
  const isPositiveChange = balanceChangePct >= 0;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Termômetro do Mercado</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Thermometer bar */}
        <div className="relative">
          <div
            className="h-3 w-full rounded-full"
            style={{
              background: "linear-gradient(to right, #EF4444, #EAB308, #22C55E)",
            }}
          />
          <div
            className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 h-5 w-5 rounded-full border-2 border-background bg-foreground shadow-md"
            style={{ left: `${indicatorPosition}%` }}
          />
        </div>

        {/* Label */}
        <p className={`text-center font-bold ${getSentimentColor(data.sentiment)}`}>
          {data.label}
        </p>

        {/* Stats */}
        <div className="grid grid-cols-3 gap-4 text-center">
          <div>
            <p className="text-xs text-muted-foreground">Compras</p>
            <p className="text-sm font-bold text-success">
              {formatCompactCurrency(data.total_buys)}
            </p>
          </div>
          <div>
            <p className="text-xs text-muted-foreground">Vendas</p>
            <p className="text-sm font-bold text-destructive">
              {formatCompactCurrency(data.total_sells)}
            </p>
          </div>
          <div>
            <p className="text-xs text-muted-foreground">Saldo</p>
            <p
              className={`text-sm font-bold ${
                data.balance >= 0 ? "text-success" : "text-destructive"
              }`}
            >
              {formatCompactCurrency(data.balance)}
            </p>
          </div>
        </div>

        {/* Variation */}
        <div className="flex items-center justify-center gap-1 text-sm">
          {isPositiveChange ? (
            <ArrowUp className="h-4 w-4 text-success" />
          ) : (
            <ArrowDown className="h-4 w-4 text-destructive" />
          )}
          <span className={isPositiveChange ? "text-success" : "text-destructive"}>
            {isFinite(balanceChangePct) ? Math.abs(balanceChangePct).toFixed(1) : "0.0"}%
          </span>
          <span className="text-muted-foreground">vs mês anterior</span>
        </div>
      </CardContent>
    </Card>
  );
}
