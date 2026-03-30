"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { ArrowUp, ArrowDown } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api";
import { formatCurrency } from "@/lib/utils";

interface ActivityRadarItem {
  company_id: number;
  company_name: string;
  ticker: string | null;
  direction: string;
  multiplier: number;
  dominant_operation: string | null;
  total_value: number;
  operations_count: number;
  insider_group: string | null;
  alert_severity: string | null;
  alert_type: string | null;
  has_correlation: boolean;
}

function formatCompactCurrency(value: number): string {
  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  if (abs >= 1e9) return `${sign}R$ ${(abs / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${sign}R$ ${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}R$ ${(abs / 1e3).toFixed(1)}K`;
  return formatCurrency(value);
}

function isBuying(item: ActivityRadarItem): boolean {
  const op = (item.dominant_operation || "").toLowerCase();
  return op.includes("compra") || op.includes("comprou") || item.direction === "buying";
}

export function ActivityRadar() {
  const [data, setData] = useState<ActivityRadarItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    api
      .get<{ data: ActivityRadarItem[] }>("/dashboard/activity-radar", {
        params: { limit: 5 },
      })
      .then((res) => setData(res.data))
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-4">
        <h2 className="text-lg font-semibold">Quem está se movendo agora</h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
          {Array.from({ length: 5 }).map((_, i) => (
            <Card key={i}>
              <CardContent className="p-4">
                <div className="animate-pulse space-y-3">
                  <div className="h-6 w-16 rounded bg-muted" />
                  <div className="h-4 w-full rounded bg-muted" />
                  <div className="h-5 w-12 rounded bg-muted" />
                  <div className="h-4 w-20 rounded bg-muted" />
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    );
  }

  if (error || data.length === 0) {
    return (
      <div className="space-y-4">
        <h2 className="text-lg font-semibold">Quem está se movendo agora</h2>
        <p className="text-center text-muted-foreground py-4">
          Sem atividade atípica nos últimos 30 dias
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <h2 className="text-lg font-semibold">Quem está se movendo agora</h2>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
        {data.map((item) => {
          const buying = isBuying(item);
          return (
            <Link key={item.company_id} href={`/empresas/${item.company_id}`}>
              <Card
                className={`border-l-4 hover:shadow-md transition-shadow cursor-pointer ${
                  buying ? "border-l-success" : "border-l-destructive"
                }`}
              >
                <CardContent className="p-4 space-y-2">
                  <span className="font-mono font-bold text-lg">
                    {item.ticker || "\u2014"}
                  </span>
                  <p className="text-xs text-muted-foreground truncate">
                    {item.company_name}
                  </p>
                  <div className="flex items-center gap-1">
                    {buying ? (
                      <ArrowUp className="h-4 w-4 text-success" />
                    ) : (
                      <ArrowDown className="h-4 w-4 text-destructive" />
                    )}
                    <span
                      className={`font-bold ${
                        buying ? "text-success" : "text-destructive"
                      }`}
                    >
                      {item.multiplier.toFixed(1)}x
                    </span>
                  </div>
                  <Badge variant={buying ? "success" : "destructive"}>
                    {buying ? "COMPRA" : "VENDA"}
                  </Badge>
                  <p className="text-sm font-mono">
                    {formatCompactCurrency(item.total_value)}
                  </p>
                  {item.insider_group && (
                    <p className="text-xs text-muted-foreground">
                      {item.insider_group}
                    </p>
                  )}
                </CardContent>
              </Card>
            </Link>
          );
        })}
      </div>
    </div>
  );
}
