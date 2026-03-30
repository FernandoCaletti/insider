import {
  Building2,
  FileText,
  ArrowLeftRight,
  TrendingUp,
  TrendingDown,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatQuantity, formatCurrency } from "@/lib/utils";

export interface DashboardSummaryExtended {
  total_companies: number;
  total_documents: number;
  total_movements: number;
  new_companies_this_month: number;
  last_sync_docs: number;
  movements_30d: number;
  movements_30d_change_pct: number;
  balance_30d: number;
  balance_30d_change_pct: number;
}

function formatCompactCurrency(value: number): string {
  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  if (abs >= 1e9) return `${sign}R$ ${(abs / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${sign}R$ ${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}R$ ${(abs / 1e3).toFixed(1)}K`;
  return formatCurrency(value);
}

function ChangeIndicator({ value }: { value: number }) {
  if (!isFinite(value) || value === 0) return null;
  const isPositive = value > 0;
  return (
    <span
      className={`text-xs ${isPositive ? "text-success" : "text-destructive"}`}
    >
      {isPositive ? "↑" : "↓"} {Math.abs(value).toFixed(1)}%
    </span>
  );
}

export function SummaryCards({ data }: { data: DashboardSummaryExtended }) {
  const balancePositive = data.balance_30d >= 0;
  const BalanceIcon = balancePositive ? TrendingUp : TrendingDown;

  const cards = [
    {
      title: "Empresas monitoradas",
      icon: Building2,
      value: formatQuantity(data.total_companies),
      sub:
        data.new_companies_this_month > 0
          ? `+${data.new_companies_this_month} este mês`
          : null,
      subColor: "text-muted-foreground",
      valueColor: "",
    },
    {
      title: "Movimentações (90d)",
      icon: ArrowLeftRight,
      value: formatQuantity(data.movements_30d),
      sub: null as string | null,
      subColor: "",
      valueColor: "",
      change: data.movements_30d_change_pct,
    },
    {
      title: "Saldo líquido (90d)",
      icon: BalanceIcon,
      value: formatCompactCurrency(data.balance_30d),
      sub: null as string | null,
      subColor: "",
      valueColor: balancePositive ? "text-success" : "text-destructive",
      change: data.balance_30d_change_pct,
    },
  ];

  return (
    <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
      {cards.map((card) => (
        <Card key={card.title}>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              {card.title}
            </CardTitle>
            <card.icon className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <p className={`text-2xl font-bold ${card.valueColor}`}>
              {card.value}
            </p>
            <div className="mt-1">
              {card.sub && (
                <p className={`text-xs ${card.subColor}`}>{card.sub}</p>
              )}
              {"change" in card && card.change != null && (
                <ChangeIndicator value={card.change} />
              )}
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
