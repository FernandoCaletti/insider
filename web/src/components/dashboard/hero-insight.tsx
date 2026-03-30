"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { Calendar, ArrowRight } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api";
import { formatCurrency } from "@/lib/utils";

interface HeroInsightData {
  type: string;
  title: string;
  subtitle: string;
  company: {
    id: number;
    name: string;
    ticker: string | null;
  };
  badges: {
    alert_type: string;
    severity: string;
    insider_group: string | null;
    operation_type: string | null;
  };
  values: {
    total_value: number | null;
    quantity: number | null;
  };
  correlation: {
    fact_subject: string | null;
    fact_date: string | null;
    days_diff: number | null;
  } | null;
}

function getSeverityVariant(severity: string) {
  switch (severity) {
    case "alta":
    case "critical":
    case "high":
      return "destructive" as const;
    case "media":
    case "medium":
      return "warning" as const;
    default:
      return "secondary" as const;
  }
}

function getOperationVariant(operationType: string | null) {
  if (!operationType) return "secondary" as const;
  const lower = operationType.toLowerCase();
  if (lower.includes("compra") || lower.includes("comprou")) return "success" as const;
  if (lower.includes("venda") || lower.includes("vendeu")) return "destructive" as const;
  return "secondary" as const;
}

export function HeroInsight() {
  const [data, setData] = useState<HeroInsightData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    api
      .get<{ data: HeroInsightData }>("/dashboard/hero-insight")
      .then((res) => setData(res.data))
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <Card>
        <CardContent className="p-6">
          <div className="animate-pulse space-y-4">
            <div className="flex items-center gap-3">
              <div className="h-8 w-20 rounded bg-muted" />
              <div className="h-6 w-64 rounded bg-muted" />
            </div>
            <div className="h-4 w-48 rounded bg-muted" />
            <div className="flex gap-2">
              <div className="h-6 w-24 rounded bg-muted" />
              <div className="h-6 w-20 rounded bg-muted" />
              <div className="h-6 w-28 rounded bg-muted" />
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error || !data || !data.company) {
    return (
      <Card>
        <CardContent className="p-6">
          <p className="text-center text-muted-foreground py-4">
            Nenhum insight disponível esta semana
          </p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardContent className="p-6 space-y-4">
        <div className="flex items-center gap-3 flex-wrap">
          {data.company.ticker && (
            <span className="font-mono font-bold bg-primary text-primary-foreground rounded px-3 py-1 text-sm">
              {data.company.ticker}
            </span>
          )}
          <h2 className="text-xl font-bold">{data.title}</h2>
        </div>

        <p className="text-sm text-muted-foreground">{data.subtitle}</p>

        <div className="flex flex-wrap gap-2">
          <Badge variant={getSeverityVariant(data.badges.severity)}>
            {data.badges.alert_type}
          </Badge>
          <Badge variant={getSeverityVariant(data.badges.severity)}>
            {data.badges.severity}
          </Badge>
          {data.badges.insider_group && (
            <Badge variant="secondary">{data.badges.insider_group}</Badge>
          )}
          {data.badges.operation_type && (
            <Badge variant={getOperationVariant(data.badges.operation_type)}>
              {data.badges.operation_type}
            </Badge>
          )}
        </div>

        {data.values.total_value != null && (
          <p className="text-sm text-muted-foreground">
            Valor: <span className="font-mono font-medium text-foreground">{formatCurrency(data.values.total_value)}</span>
          </p>
        )}

        {data.correlation && data.correlation.fact_subject && (
          <div className="flex items-center gap-2 text-sm text-muted-foreground border-t pt-3">
            <Calendar className="h-4 w-4 shrink-0" />
            <span>
              {data.correlation.fact_subject}
              {data.correlation.days_diff != null && (
                <> &mdash; {Math.abs(data.correlation.days_diff)} dias {data.correlation.days_diff < 0 ? "antes" : "depois"} do fato</>
              )}
            </span>
          </div>
        )}

        <Link
          href={`/empresas/${data.company.id}`}
          className="inline-flex items-center gap-1 text-sm font-medium text-primary hover:underline"
        >
          Ver detalhes <ArrowRight className="h-4 w-4" />
        </Link>
      </CardContent>
    </Card>
  );
}
