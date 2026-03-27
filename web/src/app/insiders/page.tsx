"use client";

import { useState, useEffect, useCallback } from "react";
import Link from "next/link";
import { useSearchParams, useRouter } from "next/navigation";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api";
import { formatCurrency, formatQuantity, formatDate } from "@/lib/utils";
import type { InsiderListItem, PaginatedResponse } from "@/lib/types";

const INSIDER_GROUPS = [
  { value: "Controlador", label: "Controlador" },
  { value: "Conselho de Administracao", label: "Conselho de Administração" },
  { value: "Diretoria", label: "Diretoria" },
  { value: "Conselho Fiscal", label: "Conselho Fiscal" },
  { value: "Orgaos Tecnicos", label: "Órgãos Técnicos" },
  { value: "Pessoas Ligadas", label: "Pessoas Ligadas" },
];

export default function InsidersPage() {
  const searchParams = useSearchParams();
  const router = useRouter();

  const [data, setData] = useState<InsiderListItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  const [search, setSearch] = useState(searchParams.get("search") || "");
  const [insiderGroup, setInsiderGroup] = useState(
    searchParams.get("insider_group") || ""
  );
  const [page, setPage] = useState(
    Number(searchParams.get("page")) || 1
  );
  const perPage = 50;

  const updateUrl = useCallback(
    (params: Record<string, string>) => {
      const sp = new URLSearchParams();
      Object.entries(params).forEach(([k, v]) => {
        if (v) sp.set(k, v);
      });
      const qs = sp.toString();
      router.replace(qs ? `?${qs}` : "/insiders", { scroll: false });
    },
    [router]
  );

  const fetchInsiders = useCallback(async () => {
    setLoading(true);
    try {
      const params: Record<string, string | number> = {
        page,
        per_page: perPage,
      };
      if (search) params.search = search;
      if (insiderGroup) params.insider_group = insiderGroup;

      const result = await api.get<PaginatedResponse<InsiderListItem>>(
        "/insiders",
        { params }
      );
      setData(result.data);
      setTotal(result.total);
    } catch {
      setData([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  }, [page, search, insiderGroup]);

  useEffect(() => {
    fetchInsiders();
  }, [fetchInsiders]);

  useEffect(() => {
    updateUrl({
      search,
      insider_group: insiderGroup,
      page: page > 1 ? String(page) : "",
    });
  }, [search, insiderGroup, page, updateUrl]);

  const totalPages = Math.ceil(total / perPage);

  return (
    <div className="container mx-auto px-4 py-8 space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Insiders</h1>
        <p className="text-muted-foreground">
          Histórico de negociações de insiders em todas as empresas
        </p>
      </div>

      <div className="flex flex-col sm:flex-row gap-3">
        <Input
          placeholder="Buscar por nome..."
          value={search}
          onChange={(e) => {
            setSearch(e.target.value);
            setPage(1);
          }}
          className="w-full sm:w-[300px]"
        />

        <Select
          value={insiderGroup || "all"}
          onValueChange={(v) => {
            setInsiderGroup(v === "all" ? "" : v);
            setPage(1);
          }}
        >
          <SelectTrigger className="w-full sm:w-[220px]">
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

      {loading ? (
        <div className="flex items-center justify-center py-16 text-muted-foreground">
          Carregando...
        </div>
      ) : data.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
          <p className="text-lg">Nenhum insider encontrado</p>
        </div>
      ) : (
        <>
          <div className="text-sm text-muted-foreground">
            {formatQuantity(total)} insider{total !== 1 ? "s" : ""} encontrado
            {total !== 1 ? "s" : ""}
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Nome</TableHead>
                <TableHead>Grupo</TableHead>
                <TableHead className="text-right">Operações</TableHead>
                <TableHead className="text-right">Compras</TableHead>
                <TableHead className="text-right">Vendas</TableHead>
                <TableHead className="text-right">Valor Total</TableHead>
                <TableHead className="text-right">Empresas</TableHead>
                <TableHead>Última Operação</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((insider) => (
                <TableRow key={insider.insider_name}>
                  <TableCell>
                    <Link
                      href={`/insiders/${encodeURIComponent(insider.insider_name)}`}
                      className="font-medium text-primary hover:underline"
                    >
                      {insider.insider_name}
                    </Link>
                  </TableCell>
                  <TableCell>
                    {insider.insider_group ? (
                      <Badge variant="secondary">
                        {insider.insider_group}
                      </Badge>
                    ) : (
                      "\u2014"
                    )}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {formatQuantity(insider.total_operations)}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {formatQuantity(insider.buy_count)}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {formatQuantity(insider.sell_count)}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {formatCurrency(insider.total_value)}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {insider.companies_count}
                  </TableCell>
                  <TableCell>
                    {insider.last_operation
                      ? formatDate(insider.last_operation)
                      : "\u2014"}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>

          {totalPages > 1 && (
            <div className="flex items-center justify-center gap-2 pt-4">
              <Button
                variant="outline"
                size="sm"
                disabled={page <= 1}
                onClick={() => setPage(page - 1)}
              >
                Anterior
              </Button>
              <span className="text-sm text-muted-foreground">
                Página {page} de {totalPages}
              </span>
              <Button
                variant="outline"
                size="sm"
                disabled={page >= totalPages}
                onClick={() => setPage(page + 1)}
              >
                Próxima
              </Button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
