"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import Link from "next/link";
import { Search, Building2 } from "lucide-react";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { api } from "@/lib/api";
import { displayTicker, formatDate, formatQuantity } from "@/lib/utils";
import type { CompanyListItem, PaginatedResponse } from "@/lib/types";

const SECTORS = [
  "Energia Elétrica",
  "Intermediários Financeiros",
  "Petróleo, Gás e Biocombustíveis",
  "Telecomunicações",
  "Mineração",
  "Siderurgia e Metalurgia",
  "Alimentos Processados",
  "Comércio",
  "Transporte",
  "Construção Civil",
  "Máquinas e Equipamentos",
  "Químicos",
  "Têxtil, Vestuário e Calçados",
  "Saúde",
  "Papel e Celulose",
];

export default function EmpresasPage() {
  const [search, setSearch] = useState("");
  const [sector, setSector] = useState<string>("");
  const [showActive, setShowActive] = useState<boolean | null>(null);
  const [data, setData] = useState<CompanyListItem[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [perPage] = useState(20);
  const [loading, setLoading] = useState(true);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const fetchCompanies = useCallback(
    async (searchTerm: string, sectorFilter: string, activeFilter: boolean | null, pageNum: number) => {
      setLoading(true);
      try {
        const params: Record<string, string | number | boolean> = {
          page: pageNum,
          per_page: perPage,
        };
        if (searchTerm) params.search = searchTerm;
        if (sectorFilter) params.sector = sectorFilter;
        if (activeFilter !== null) params.is_active = activeFilter;

        const result = await api.get<PaginatedResponse<CompanyListItem>>(
          "/companies",
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
    },
    [perPage]
  );

  useEffect(() => {
    fetchCompanies(search, sector, showActive, page);
  }, [sector, showActive, page, fetchCompanies]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleSearchChange = (value: string) => {
    setSearch(value);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      setPage(1);
      fetchCompanies(value, sector, showActive, 1);
    }, 300);
  };

  const handleSectorChange = (value: string) => {
    const newSector = value === "all" ? "" : value;
    setSector(newSector);
    setPage(1);
  };

  const handleActiveToggle = () => {
    const next = showActive === null ? true : showActive === true ? false : null;
    setShowActive(next);
    setPage(1);
  };

  const totalPages = Math.ceil(total / perPage);

  return (
    <div className="container mx-auto px-4 py-8 space-y-6">
      <h1 className="text-2xl font-bold">Empresas</h1>

      {/* Filters */}
      <div className="flex flex-col sm:flex-row gap-3">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Buscar por nome, ticker ou codigo CVM..."
            value={search}
            onChange={(e) => handleSearchChange(e.target.value)}
            className="pl-9"
          />
        </div>
        <Select value={sector || "all"} onValueChange={handleSectorChange}>
          <SelectTrigger className="w-full sm:w-[220px]">
            <SelectValue placeholder="Todos os setores" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">Todos os setores</SelectItem>
            {SECTORS.map((s) => (
              <SelectItem key={s} value={s}>
                {s}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Button
          variant={showActive === null ? "outline" : "default"}
          onClick={handleActiveToggle}
          className="whitespace-nowrap"
        >
          {showActive === null
            ? "Todas"
            : showActive
              ? "Ativas"
              : "Inativas"}
        </Button>
      </div>

      {/* Results */}
      {loading ? (
        <div className="flex items-center justify-center py-16 text-muted-foreground">
          Carregando...
        </div>
      ) : data.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground space-y-3">
          <Building2 className="h-12 w-12" />
          <p className="text-lg">Nenhuma empresa encontrada</p>
          <p className="text-sm">Tente ajustar os filtros ou termo de busca.</p>
        </div>
      ) : (
        <>
          <div className="text-sm text-muted-foreground">
            {formatQuantity(total)} empresa{total !== 1 ? "s" : ""} encontrada{total !== 1 ? "s" : ""}
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Nome</TableHead>
                <TableHead>Ticker</TableHead>
                <TableHead>Setor</TableHead>
                <TableHead className="text-right">Documentos</TableHead>
                <TableHead>Último documento</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((company) => (
                <TableRow key={company.id}>
                  <TableCell>
                    <Link
                      href={`/empresas/${company.id}`}
                      className="text-primary hover:underline font-medium"
                    >
                      {company.name}
                    </Link>
                    {!company.is_active && (
                      <Badge variant="secondary" className="ml-2 text-xs">
                        Inativa
                      </Badge>
                    )}
                  </TableCell>
                  <TableCell className="font-mono text-sm">
                    {displayTicker(company.ticker)}
                  </TableCell>
                  <TableCell className="text-sm text-muted-foreground">
                    {company.sector || "\u2014"}
                  </TableCell>
                  <TableCell className="text-right font-mono">
                    {formatQuantity(company.total_documents)}
                  </TableCell>
                  <TableCell>
                    {company.last_document
                      ? formatDate(company.last_document)
                      : "\u2014"}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>

          {/* Pagination */}
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
