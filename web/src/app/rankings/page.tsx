"use client";

import { useState, useEffect, useCallback } from "react";
import Link from "next/link";
import { Trophy, TrendingDown, Activity, BarChart3, Users, Building2, Bell } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api";
import {
  formatCurrency,
  formatQuantity,
  displayTicker,
} from "@/lib/utils";
import type { RankingEntry, LargestPosition, RoleRankingEntry, BrokerRankingEntry, AlertRankingEntry } from "@/lib/types";

const PERIODS = [
  { value: "7d", label: "7 dias" },
  { value: "30d", label: "30 dias" },
  { value: "90d", label: "90 dias" },
  { value: "12m", label: "12 meses" },
  { value: "all", label: "Todo periodo" },
];

const ASSET_TYPES = [
  { value: "all", label: "Todos" },
  { value: "ACAO_ON", label: "Ação ON" },
  { value: "ACAO_PN", label: "Ação PN" },
  { value: "DEBENTURE", label: "Debenture" },
  { value: "OPCAO", label: "Opção" },
  { value: "OPCAO_COMPRA", label: "Opção de Compra" },
  { value: "OPCAO_VENDA", label: "Opção de Venda" },
  { value: "BDR", label: "BDR" },
  { value: "UNIT", label: "Unit" },
  { value: "OUTRO", label: "Outro" },
];

const INSIDER_GROUPS = [
  { value: "Controlador", label: "Controlador" },
  { value: "Conselho de Administracao", label: "Conselho de Administração" },
  { value: "Diretoria", label: "Diretoria" },
  { value: "Conselho Fiscal", label: "Conselho Fiscal" },
  { value: "Orgaos Tecnicos", label: "Órgãos Técnicos" },
  { value: "Pessoas Ligadas", label: "Pessoas Ligadas" },
];

interface RankingResponse {
  data: RankingEntry[];
  period: string;
}

interface PositionResponse {
  data: LargestPosition[];
}

interface RoleRankingResponse {
  data: RoleRankingEntry[];
  period: string;
}

interface BrokerRankingResponse {
  data: BrokerRankingEntry[];
  period: string;
}

interface AlertRankingResponse {
  data: AlertRankingEntry[];
  period: string;
}

const ALERT_TYPES = [
  { value: "all", label: "Todos" },
  { value: "alto_valor", label: "Alto Valor" },
  { value: "volume_atipico", label: "Volume Atípico" },
  { value: "mudanca_direcao", label: "Mudança de Direção" },
  { value: "retorno_atividade", label: "Retorno de Atividade" },
];

const SEVERITIES = [
  { value: "all", label: "Todas" },
  { value: "critical", label: "Crítica" },
  { value: "high", label: "Alta" },
  { value: "medium", label: "Média" },
  { value: "low", label: "Baixa" },
];

export default function RankingsPage() {
  const [period, setPeriod] = useState("30d");
  const [insiderGroup, setInsiderGroup] = useState("all");
  const [positionAssetType, setPositionAssetType] = useState("all");
  const [buyers, setBuyers] = useState<RankingEntry[]>([]);
  const [sellers, setSellers] = useState<RankingEntry[]>([]);
  const [active, setActive] = useState<RankingEntry[]>([]);
  const [positions, setPositions] = useState<LargestPosition[]>([]);
  const [roles, setRoles] = useState<RoleRankingEntry[]>([]);
  const [brokers, setBrokers] = useState<BrokerRankingEntry[]>([]);
  const [alertRankings, setAlertRankings] = useState<AlertRankingEntry[]>([]);
  const [alertType, setAlertType] = useState("all");
  const [alertSeverity, setAlertSeverity] = useState("all");
  const [loading, setLoading] = useState(true);
  const [positionsLoading, setPositionsLoading] = useState(true);
  const [rolesLoading, setRolesLoading] = useState(true);
  const [brokersLoading, setBrokersLoading] = useState(true);
  const [alertsLoading, setAlertsLoading] = useState(true);

  const fetchRankings = useCallback(async (p: string, group: string) => {
    setLoading(true);
    try {
      const baseParams: Record<string, string | number> = { period: p, limit: 20 };
      if (group !== "all") baseParams.insider_group = group;

      const [buyersRes, sellersRes, activeRes] = await Promise.allSettled([
        api.get<RankingResponse>("/rankings/top-buyers", {
          params: baseParams,
        }),
        api.get<RankingResponse>("/rankings/top-sellers", {
          params: baseParams,
        }),
        api.get<RankingResponse>("/rankings/most-active", {
          params: baseParams,
        }),
      ]);
      setBuyers(
        buyersRes.status === "fulfilled" ? buyersRes.value.data : []
      );
      setSellers(
        sellersRes.status === "fulfilled" ? sellersRes.value.data : []
      );
      setActive(
        activeRes.status === "fulfilled" ? activeRes.value.data : []
      );
    } catch {
      setBuyers([]);
      setSellers([]);
      setActive([]);
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchRoles = useCallback(async (p: string) => {
    setRolesLoading(true);
    try {
      const res = await api.get<RoleRankingResponse>("/rankings/by-role", {
        params: { period: p, limit: 20 },
      });
      setRoles(res.data);
    } catch {
      setRoles([]);
    } finally {
      setRolesLoading(false);
    }
  }, []);

  const fetchBrokers = useCallback(async (p: string, group: string) => {
    setBrokersLoading(true);
    try {
      const params: Record<string, string | number> = { period: p, limit: 20 };
      if (group !== "all") params.insider_group = group;
      const res = await api.get<BrokerRankingResponse>("/rankings/by-broker", {
        params,
      });
      setBrokers(res.data);
    } catch {
      setBrokers([]);
    } finally {
      setBrokersLoading(false);
    }
  }, []);

  const fetchAlertRankings = useCallback(async (p: string, aType: string, sev: string) => {
    setAlertsLoading(true);
    try {
      const params: Record<string, string | number> = { period: p, limit: 20 };
      if (aType !== "all") params.alert_type = aType;
      if (sev !== "all") params.severity = sev;
      const res = await api.get<AlertRankingResponse>("/rankings/by-alerts", {
        params,
      });
      setAlertRankings(res.data);
    } catch {
      setAlertRankings([]);
    } finally {
      setAlertsLoading(false);
    }
  }, []);

  const fetchPositions = useCallback(async (assetType: string) => {
    setPositionsLoading(true);
    try {
      const params: Record<string, string | number> = { limit: 20 };
      if (assetType !== "all") params.asset_type = assetType;
      const res = await api.get<PositionResponse>("/rankings/largest-positions", {
        params,
      });
      setPositions(res.data);
    } catch {
      setPositions([]);
    } finally {
      setPositionsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchRankings(period, insiderGroup);
  }, [period, insiderGroup, fetchRankings]);

  useEffect(() => {
    fetchRoles(period);
  }, [period, fetchRoles]);

  useEffect(() => {
    fetchBrokers(period, insiderGroup);
  }, [period, insiderGroup, fetchBrokers]);

  useEffect(() => {
    fetchAlertRankings(period, alertType, alertSeverity);
  }, [period, alertType, alertSeverity, fetchAlertRankings]);

  useEffect(() => {
    fetchPositions(positionAssetType);
  }, [positionAssetType, fetchPositions]);

  const handlePeriodChange = (p: string) => {
    setPeriod(p);
  };

  return (
    <div className="container mx-auto px-4 py-8 space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <h1 className="text-2xl font-bold">Rankings</h1>

        {/* Period selector + Insider Group filter */}
        <div className="flex gap-3 flex-wrap items-center">
          <div className="flex gap-1 flex-wrap">
            {PERIODS.map((p) => (
              <Button
                key={p.value}
                variant={period === p.value ? "default" : "outline"}
                size="sm"
                onClick={() => handlePeriodChange(p.value)}
              >
                {p.label}
              </Button>
            ))}
          </div>
          <Select value={insiderGroup} onValueChange={setInsiderGroup}>
            <SelectTrigger className="w-[220px]">
              <SelectValue placeholder="Grupo do Insider" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todos os grupos</SelectItem>
              {INSIDER_GROUPS.map((g) => (
                <SelectItem key={g.value} value={g.value}>
                  {g.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      <Tabs defaultValue="buyers">
        <TabsList className="w-full sm:w-auto">
          <TabsTrigger value="buyers" className="gap-1.5">
            <Trophy className="h-4 w-4" />
            Maiores Compradores
          </TabsTrigger>
          <TabsTrigger value="sellers" className="gap-1.5">
            <TrendingDown className="h-4 w-4" />
            Maiores Vendedores
          </TabsTrigger>
          <TabsTrigger value="active" className="gap-1.5">
            <Activity className="h-4 w-4" />
            Mais Ativas
          </TabsTrigger>
          <TabsTrigger value="roles" className="gap-1.5">
            <Users className="h-4 w-4" />
            Por Cargo
          </TabsTrigger>
          <TabsTrigger value="brokers" className="gap-1.5">
            <Building2 className="h-4 w-4" />
            Por Corretora
          </TabsTrigger>
          <TabsTrigger value="alerts" className="gap-1.5">
            <Bell className="h-4 w-4" />
            Por Alertas
          </TabsTrigger>
          <TabsTrigger value="positions" className="gap-1.5">
            <BarChart3 className="h-4 w-4" />
            Maiores Posições
          </TabsTrigger>
        </TabsList>

        {/* Top Buyers */}
        <TabsContent value="buyers">
          {loading ? (
            <LoadingState />
          ) : buyers.length === 0 ? (
            <EmptyState />
          ) : (
            <RankingTable data={buyers} showQuantity />
          )}
        </TabsContent>

        {/* Top Sellers */}
        <TabsContent value="sellers">
          {loading ? (
            <LoadingState />
          ) : sellers.length === 0 ? (
            <EmptyState />
          ) : (
            <RankingTable data={sellers} showQuantity />
          )}
        </TabsContent>

        {/* Most Active */}
        <TabsContent value="active">
          {loading ? (
            <LoadingState />
          ) : active.length === 0 ? (
            <EmptyState />
          ) : (
            <RankingTable data={active} showQuantity={false} />
          )}
        </TabsContent>

        {/* By Role */}
        <TabsContent value="roles">
          {rolesLoading ? (
            <LoadingState />
          ) : roles.length === 0 ? (
            <EmptyState />
          ) : (
            <RolesTable data={roles} />
          )}
        </TabsContent>

        {/* By Broker */}
        <TabsContent value="brokers">
          {brokersLoading ? (
            <LoadingState />
          ) : brokers.length === 0 ? (
            <EmptyState />
          ) : (
            <BrokersTable data={brokers} />
          )}
        </TabsContent>

        {/* By Alerts */}
        <TabsContent value="alerts">
          <div className="mb-4 flex gap-3 flex-wrap">
            <Select value={alertType} onValueChange={setAlertType}>
              <SelectTrigger className="w-[200px]">
                <SelectValue placeholder="Tipo de alerta" />
              </SelectTrigger>
              <SelectContent>
                {ALERT_TYPES.map((t) => (
                  <SelectItem key={t.value} value={t.value}>
                    {t.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={alertSeverity} onValueChange={setAlertSeverity}>
              <SelectTrigger className="w-[200px]">
                <SelectValue placeholder="Severidade" />
              </SelectTrigger>
              <SelectContent>
                {SEVERITIES.map((s) => (
                  <SelectItem key={s.value} value={s.value}>
                    {s.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          {alertsLoading ? (
            <LoadingState />
          ) : alertRankings.length === 0 ? (
            <EmptyState />
          ) : (
            <AlertsRankingTable data={alertRankings} />
          )}
        </TabsContent>

        {/* Largest Positions */}
        <TabsContent value="positions">
          <div className="mb-4">
            <Select
              value={positionAssetType}
              onValueChange={setPositionAssetType}
            >
              <SelectTrigger className="w-[200px]">
                <SelectValue placeholder="Tipo de ativo" />
              </SelectTrigger>
              <SelectContent>
                {ASSET_TYPES.map((t) => (
                  <SelectItem key={t.value} value={t.value}>
                    {t.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          {positionsLoading ? (
            <LoadingState />
          ) : positions.length === 0 ? (
            <EmptyState />
          ) : (
            <PositionsTable data={positions} />
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}

function LoadingState() {
  return (
    <div className="flex items-center justify-center py-16 text-muted-foreground">
      Carregando...
    </div>
  );
}

function EmptyState() {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-muted-foreground space-y-2">
      <p className="text-lg">Nenhum dado encontrado</p>
      <p className="text-sm">Tente selecionar um periodo diferente.</p>
    </div>
  );
}

function RankingTable({
  data,
  showQuantity,
}: {
  data: RankingEntry[];
  showQuantity: boolean;
}) {
  return (
    <div className="border rounded-md overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[50px]">#</TableHead>
            <TableHead>Empresa</TableHead>
            <TableHead>Ticker</TableHead>
            <TableHead className="text-right">N. Operações</TableHead>
            <TableHead className="text-right">Valor Total</TableHead>
            {showQuantity && (
              <TableHead className="text-right">Quantidade</TableHead>
            )}
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((entry, index) => (
            <TableRow key={entry.company_id}>
              <TableCell className="font-medium text-muted-foreground">
                {index + 1}
              </TableCell>
              <TableCell>
                <Link
                  href={`/empresas/${entry.company_id}`}
                  className="text-primary hover:underline font-medium"
                >
                  {entry.company_name}
                </Link>
              </TableCell>
              <TableCell className="font-mono text-sm">
                {displayTicker(entry.company_ticker)}
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatQuantity(entry.total_operations)}
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatCurrency(entry.total_value)}
              </TableCell>
              {showQuantity && (
                <TableCell className="text-right font-mono">
                  {formatQuantity(entry.total_quantity)}
                </TableCell>
              )}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function RolesTable({ data }: { data: RoleRankingEntry[] }) {
  return (
    <div className="border rounded-md overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[50px]">#</TableHead>
            <TableHead>Cargo</TableHead>
            <TableHead className="text-right">Operações</TableHead>
            <TableHead className="text-right">Compras</TableHead>
            <TableHead className="text-right">Vendas</TableHead>
            <TableHead className="text-right">Empresas</TableHead>
            <TableHead className="text-right">Valor Total</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((entry, index) => (
            <TableRow key={entry.insider_group}>
              <TableCell className="font-medium text-muted-foreground">
                {index + 1}
              </TableCell>
              <TableCell className="font-medium">
                {entry.insider_group}
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatQuantity(entry.total_operations)}
              </TableCell>
              <TableCell className="text-right font-mono text-green-600">
                {formatQuantity(entry.buy_count)}
              </TableCell>
              <TableCell className="text-right font-mono text-red-600">
                {formatQuantity(entry.sell_count)}
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatQuantity(entry.companies_count)}
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatCurrency(entry.total_value)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function BrokersTable({ data }: { data: BrokerRankingEntry[] }) {
  return (
    <div className="border rounded-md overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[50px]">#</TableHead>
            <TableHead>Corretora</TableHead>
            <TableHead className="text-right">Operações</TableHead>
            <TableHead className="text-right">Compras</TableHead>
            <TableHead className="text-right">Vendas</TableHead>
            <TableHead className="text-right">Empresas</TableHead>
            <TableHead className="text-right">Valor Total</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((entry, index) => (
            <TableRow key={entry.broker}>
              <TableCell className="font-medium text-muted-foreground">
                {index + 1}
              </TableCell>
              <TableCell className="font-medium">
                {entry.broker}
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatQuantity(entry.total_operations)}
              </TableCell>
              <TableCell className="text-right font-mono text-green-600">
                {formatQuantity(entry.buy_count)}
              </TableCell>
              <TableCell className="text-right font-mono text-red-600">
                {formatQuantity(entry.sell_count)}
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatQuantity(entry.companies_count)}
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatCurrency(entry.total_value)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function AlertsRankingTable({ data }: { data: AlertRankingEntry[] }) {
  return (
    <div className="border rounded-md overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[50px]">#</TableHead>
            <TableHead>Empresa</TableHead>
            <TableHead>Ticker</TableHead>
            <TableHead className="text-right">Total Alertas</TableHead>
            <TableHead className="text-right">Críticos</TableHead>
            <TableHead className="text-right">Altos</TableHead>
            <TableHead className="text-right">Não Lidos</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((entry, index) => (
            <TableRow key={entry.company_id}>
              <TableCell className="font-medium text-muted-foreground">
                {index + 1}
              </TableCell>
              <TableCell>
                <Link
                  href={`/empresas/${entry.company_id}`}
                  className="text-primary hover:underline font-medium"
                >
                  {entry.company_name}
                </Link>
              </TableCell>
              <TableCell className="font-mono text-sm">
                {displayTicker(entry.ticker)}
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatQuantity(entry.alert_count)}
              </TableCell>
              <TableCell className="text-right font-mono text-red-600">
                {entry.critical_count > 0 ? formatQuantity(entry.critical_count) : "\u2014"}
              </TableCell>
              <TableCell className="text-right font-mono text-orange-600">
                {entry.high_count > 0 ? formatQuantity(entry.high_count) : "\u2014"}
              </TableCell>
              <TableCell className="text-right font-mono">
                {entry.unread_count > 0 ? formatQuantity(entry.unread_count) : "\u2014"}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

function PositionsTable({ data }: { data: LargestPosition[] }) {
  return (
    <div className="border rounded-md overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[50px]">#</TableHead>
            <TableHead>Empresa</TableHead>
            <TableHead>Ticker</TableHead>
            <TableHead>Ativo</TableHead>
            <TableHead className="text-right">Quantidade</TableHead>
            <TableHead className="text-right">Valor Estimado</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((entry, index) => (
            <TableRow key={`${entry.company_id}-${entry.asset_type}-${index}`}>
              <TableCell className="font-medium text-muted-foreground">
                {index + 1}
              </TableCell>
              <TableCell>
                <Link
                  href={`/empresas/${entry.company_id}`}
                  className="text-primary hover:underline font-medium"
                >
                  {entry.company_name}
                </Link>
              </TableCell>
              <TableCell className="font-mono text-sm">
                {displayTicker(entry.company_ticker)}
              </TableCell>
              <TableCell>
                <span className="font-medium">{entry.asset_type}</span>
                {entry.asset_description && (
                  <span className="text-sm text-muted-foreground ml-1">
                    - {entry.asset_description}
                  </span>
                )}
              </TableCell>
              <TableCell className="text-right font-mono">
                {formatQuantity(entry.total_quantity)}
              </TableCell>
              <TableCell className="text-right font-mono">
                {entry.estimated_value != null
                  ? formatCurrency(entry.estimated_value)
                  : "\u2014"}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
