"use client";

import { Suspense, useState, useEffect, useRef, useMemo } from "react";
import { useSearchParams, useRouter, usePathname } from "next/navigation";
import Link from "next/link";
import {
  Download,
  Search,
  X,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  Filter,
} from "lucide-react";
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  type SortingState,
  type ColumnDef,
  type Column,
} from "@tanstack/react-table";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
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
import { api } from "@/lib/api";
import {
  formatCurrency,
  formatDate,
  formatQuantity,
  displayTicker,
} from "@/lib/utils";
import type {
  Holding,
  HoldingsResponse,
  Company,
  PaginatedResponse,
} from "@/lib/types";

const ASSET_TYPES = [
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

const PER_PAGE_OPTIONS = [20, 50, 100];

const SORT_FIELD_MAP: Record<string, string> = {
  company_name: "company_name",
  company_ticker: "company_ticker",
  operation_date: "operation_date",
  asset_type: "asset_type",
  operation_type: "operation_type",
  quantity: "quantity",
  unit_price: "unit_price",
  total_value: "total_value",
  broker: "broker",
};

function SortHeader({
  column,
  children,
  className,
}: {
  column: Column<Holding, unknown>;
  children: React.ReactNode;
  className?: string;
}) {
  const sorted = column.getIsSorted();
  return (
    <button
      className={`flex items-center gap-1 hover:text-foreground ${className || ""}`}
      onClick={() => column.toggleSorting(sorted === "asc")}
    >
      {children}
      {sorted === "asc" ? (
        <ArrowUp className="h-3 w-3" />
      ) : sorted === "desc" ? (
        <ArrowDown className="h-3 w-3" />
      ) : (
        <ArrowUpDown className="h-3 w-3 opacity-40" />
      )}
    </button>
  );
}

interface FetchParams {
  companyId: number | null;
  assetTypes: string[];
  operationType: string;
  insiderGroup: string;
  dateFrom: string;
  dateTo: string;
  valueMin: string;
  valueMax: string;
  page: number;
  perPage: number;
  sorting: SortingState;
}

function MovimentaçõesContent() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const pathname = usePathname();

  // Filter state — initialized from URL
  const [companyId, setCompanyId] = useState<number | null>(() => {
    const v = searchParams.get("company_id");
    return v ? parseInt(v, 10) : null;
  });
  const [companyName, setCompanyName] = useState("");
  const [companySearch, setCompanySearch] = useState("");
  const [companySuggestions, setCompanySuggestions] = useState<
    Array<{ id: number; name: string; ticker: string | null }>
  >([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const autocompleteRef = useRef<HTMLDivElement>(null);

  const [assetTypes, setAssetTypes] = useState<string[]>(() => {
    const v = searchParams.get("asset_type");
    return v ? v.split(",") : [];
  });
  const [operationType, setOperationType] = useState(
    () => searchParams.get("operation_type") || ""
  );
  const [insiderGroup, setInsiderGroup] = useState(
    () => searchParams.get("insider_group") || ""
  );
  const [dateFrom, setDateFrom] = useState(
    () => searchParams.get("date_from") || ""
  );
  const [dateTo, setDateTo] = useState(
    () => searchParams.get("date_to") || ""
  );
  const [valueMin, setValueMin] = useState(
    () => searchParams.get("value_min") || ""
  );
  const [valueMax, setValueMax] = useState(
    () => searchParams.get("value_max") || ""
  );

  // Table state
  const [data, setData] = useState<Holding[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(() => {
    const v = searchParams.get("page");
    return v ? parseInt(v, 10) : 1;
  });
  const [perPage, setPerPage] = useState(() => {
    const v = searchParams.get("per_page");
    const pp = v ? parseInt(v, 10) : 20;
    return PER_PAGE_OPTIONS.includes(pp) ? pp : 20;
  });
  const [sorting, setSorting] = useState<SortingState>(() => {
    const field = searchParams.get("sort_by") || "operation_date";
    const order = searchParams.get("sort_order") || "desc";
    return [{ id: field, desc: order !== "asc" }];
  });
  const [loading, setLoading] = useState(true);

  // URL updater
  const updateUrl = (params: Record<string, string | number | null>) => {
    const sp = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) {
      if (v !== null && v !== "" && v !== undefined) sp.set(k, String(v));
    }
    const qs = sp.toString();
    router.replace(`${pathname}${qs ? `?${qs}` : ""}`, { scroll: false });
  };

  // Core fetch
  const doFetch = async (fp: FetchParams) => {
    setLoading(true);
    const sortField =
      fp.sorting.length > 0
        ? SORT_FIELD_MAP[fp.sorting[0].id] || "operation_date"
        : "operation_date";
    const sortOrder =
      fp.sorting.length > 0
        ? fp.sorting[0].desc
          ? "desc"
          : "asc"
        : "desc";

    try {
      const params: Record<string, string | number> = {
        section: "movimentacoes",
        page: fp.page,
        per_page: fp.perPage,
        sort_by: sortField,
        sort_order: sortOrder,
      };
      if (fp.companyId) params.company_id = fp.companyId;
      if (fp.assetTypes.length) params.asset_type = fp.assetTypes.join(",");
      if (fp.operationType) params.operation_type = fp.operationType;
      if (fp.insiderGroup) params.insider_group = fp.insiderGroup;
      if (fp.dateFrom) params.date_from = fp.dateFrom;
      if (fp.dateTo) params.date_to = fp.dateTo;
      if (fp.valueMin) params.value_min = parseFloat(fp.valueMin);
      if (fp.valueMax) params.value_max = parseFloat(fp.valueMax);

      const result = await api.get<HoldingsResponse>("/holdings", { params });
      setData(result.data);
      setTotal(result.total);

      updateUrl({
        company_id: fp.companyId,
        asset_type: fp.assetTypes.length ? fp.assetTypes.join(",") : null,
        operation_type: fp.operationType || null,
        insider_group: fp.insiderGroup || null,
        date_from: fp.dateFrom || null,
        date_to: fp.dateTo || null,
        value_min: fp.valueMin || null,
        value_max: fp.valueMax || null,
        page: fp.page > 1 ? fp.page : null,
        per_page: fp.perPage !== 20 ? fp.perPage : null,
        sort_by: sortField !== "operation_date" ? sortField : null,
        sort_order: sortOrder !== "desc" ? sortOrder : null,
      });
    } catch {
      setData([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  };

  // Initial load + resolve company name from URL id
  useEffect(() => {
    const initCompanyId = (() => {
      const v = searchParams.get("company_id");
      return v ? parseInt(v, 10) : null;
    })();

    if (initCompanyId) {
      api
        .get<Company>(`/companies/${initCompanyId}`)
        .then((c) => setCompanyName(c.name))
        .catch(() => {});
    }

    doFetch({
      companyId: initCompanyId,
      assetTypes: searchParams.get("asset_type")?.split(",") || [],
      operationType: searchParams.get("operation_type") || "",
      insiderGroup: searchParams.get("insider_group") || "",
      dateFrom: searchParams.get("date_from") || "",
      dateTo: searchParams.get("date_to") || "",
      valueMin: searchParams.get("value_min") || "",
      valueMax: searchParams.get("value_max") || "",
      page: parseInt(searchParams.get("page") || "1", 10),
      perPage: (() => {
        const v = searchParams.get("per_page");
        const pp = v ? parseInt(v, 10) : 20;
        return PER_PAGE_OPTIONS.includes(pp) ? pp : 20;
      })(),
      sorting: (() => {
        const field = searchParams.get("sort_by") || "operation_date";
        const order = searchParams.get("sort_order") || "desc";
        return [{ id: field, desc: order !== "asc" }];
      })(),
    });
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Filter actions
  const applyFilters = () => {
    setPage(1);
    doFetch({
      companyId,
      assetTypes,
      operationType,
      insiderGroup,
      dateFrom,
      dateTo,
      valueMin,
      valueMax,
      page: 1,
      perPage,
      sorting,
    });
  };

  const clearFilters = () => {
    const defaults: FetchParams = {
      companyId: null,
      assetTypes: [],
      operationType: "",
      insiderGroup: "",
      dateFrom: "",
      dateTo: "",
      valueMin: "",
      valueMax: "",
      page: 1,
      perPage: 20,
      sorting: [{ id: "operation_date", desc: true }],
    };
    setCompanyId(null);
    setCompanyName("");
    setCompanySearch("");
    setAssetTypes([]);
    setOperationType("");
    setInsiderGroup("");
    setDateFrom("");
    setDateTo("");
    setValueMin("");
    setValueMax("");
    setPage(1);
    setPerPage(20);
    setSorting(defaults.sorting);
    doFetch(defaults);
  };

  // Company autocomplete
  const searchCompanies = async (term: string) => {
    if (term.length < 2) {
      setCompanySuggestions([]);
      return;
    }
    try {
      const r = await api.get<PaginatedResponse<Company>>("/companies", {
        params: { search: term, per_page: 8 },
      });
      setCompanySuggestions(
        r.data.map((c) => ({ id: c.id, name: c.name, ticker: c.ticker }))
      );
      setShowSuggestions(true);
    } catch {
      setCompanySuggestions([]);
    }
  };

  const handleCompanySearch = (value: string) => {
    setCompanySearch(value);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => searchCompanies(value), 300);
  };

  const selectCompany = (c: { id: number; name: string }) => {
    setCompanyId(c.id);
    setCompanyName(c.name);
    setCompanySearch("");
    setShowSuggestions(false);
  };

  const clearCompany = () => {
    setCompanyId(null);
    setCompanyName("");
    setCompanySearch("");
  };

  // Close suggestions on outside click
  useEffect(() => {
    const handle = (e: MouseEvent) => {
      if (
        autocompleteRef.current &&
        !autocompleteRef.current.contains(e.target as Node)
      ) {
        setShowSuggestions(false);
      }
    };
    document.addEventListener("mousedown", handle);
    return () => document.removeEventListener("mousedown", handle);
  }, []);

  // Asset type toggle
  const toggleAssetType = (type: string) => {
    setAssetTypes((prev) =>
      prev.includes(type) ? prev.filter((t) => t !== type) : [...prev, type]
    );
  };

  // Sorting change — fetch immediately
  const handleSortingChange = (
    updaterOrValue: SortingState | ((old: SortingState) => SortingState)
  ) => {
    const newSorting =
      typeof updaterOrValue === "function"
        ? updaterOrValue(sorting)
        : updaterOrValue;
    setSorting(newSorting);
    setPage(1);
    doFetch({
      companyId,
      assetTypes,
      operationType,
      insiderGroup,
      dateFrom,
      dateTo,
      valueMin,
      valueMax,
      page: 1,
      perPage,
      sorting: newSorting,
    });
  };

  // Pagination — fetch immediately
  const handlePageChange = (newPage: number) => {
    setPage(newPage);
    doFetch({
      companyId,
      assetTypes,
      operationType,
      insiderGroup,
      dateFrom,
      dateTo,
      valueMin,
      valueMax,
      page: newPage,
      perPage,
      sorting,
    });
  };

  const handlePerPageChange = (newPerPage: number) => {
    setPerPage(newPerPage);
    setPage(1);
    doFetch({
      companyId,
      assetTypes,
      operationType,
      insiderGroup,
      dateFrom,
      dateTo,
      valueMin,
      valueMax,
      page: 1,
      perPage: newPerPage,
      sorting,
    });
  };

  // Export handler for CSV and XLSX
  const handleExport = (format: "csv" | "xlsx") => {
    if (total > 10000) {
      if (
        !confirm(
          "Existem mais de 10.000 registros. O arquivo exportado conterá apenas os 10.000 primeiros. Deseja continuar?"
        )
      ) {
        return;
      }
    }
    const baseUrl =
      process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";
    const sp = new URLSearchParams();
    sp.set("section", "movimentacoes");
    if (companyId) sp.set("company_id", String(companyId));
    if (assetTypes.length) sp.set("asset_type", assetTypes.join(","));
    if (operationType) sp.set("operation_type", operationType);
    if (insiderGroup) sp.set("insider_group", insiderGroup);
    if (dateFrom) sp.set("date_from", dateFrom);
    if (dateTo) sp.set("date_to", dateTo);
    if (valueMin) sp.set("value_min", valueMin);
    if (valueMax) sp.set("value_max", valueMax);
    const sortField =
      sorting.length > 0
        ? SORT_FIELD_MAP[sorting[0].id] || "operation_date"
        : "operation_date";
    const sortOrder =
      sorting.length > 0 ? (sorting[0].desc ? "desc" : "asc") : "desc";
    sp.set("sort_by", sortField);
    sp.set("sort_order", sortOrder);
    const endpoint =
      format === "xlsx" ? "/holdings/export/xlsx" : "/holdings/export";
    window.open(`${baseUrl}${endpoint}?${sp.toString()}`, "_blank");
  };

  // TanStack Table columns
  const columns = useMemo<ColumnDef<Holding, unknown>[]>(
    () => [
      {
        accessorKey: "company_name",
        header: ({ column }) => (
          <SortHeader column={column}>Empresa</SortHeader>
        ),
        cell: ({ row }) => (
          <Link
            href={`/empresas/${row.original.company_id}`}
            className="text-primary hover:underline font-medium"
          >
            {row.original.company_name}
          </Link>
        ),
      },
      {
        accessorKey: "company_ticker",
        header: ({ column }) => (
          <SortHeader column={column}>Ticker</SortHeader>
        ),
        cell: ({ row }) => (
          <span className="font-mono text-sm">
            {displayTicker(row.original.company_ticker)}
          </span>
        ),
      },
      {
        accessorKey: "operation_date",
        header: ({ column }) => <SortHeader column={column}>Data</SortHeader>,
        cell: ({ row }) =>
          row.original.operation_date
            ? formatDate(row.original.operation_date)
            : row.original.reference_date
              ? formatDate(row.original.reference_date)
              : "\u2014",
      },
      {
        accessorKey: "asset_type",
        header: ({ column }) => <SortHeader column={column}>Ativo</SortHeader>,
        cell: ({ row }) => (
          <>
            <span className="font-medium">{row.original.asset_type}</span>
            {row.original.asset_description && (
              <span className="text-sm text-muted-foreground ml-1">
                - {row.original.asset_description}
              </span>
            )}
          </>
        ),
      },
      {
        accessorKey: "operation_type",
        header: ({ column }) => (
          <SortHeader column={column}>Operação</SortHeader>
        ),
        cell: ({ row }) =>
          row.original.operation_type ? (
            <Badge
              variant={
                row.original.operation_type === "Compra"
                  ? "success"
                  : "destructive"
              }
            >
              {row.original.operation_type}
            </Badge>
          ) : (
            "\u2014"
          ),
      },
      {
        accessorKey: "insider_group",
        header: () => "Grupo",
        cell: ({ row }) => (
          <span className="text-sm">
            {row.original.insider_group || "\u2014"}
          </span>
        ),
      },
      {
        accessorKey: "quantity",
        header: ({ column }) => (
          <SortHeader column={column} className="justify-end">
            Quantidade
          </SortHeader>
        ),
        cell: ({ row }) => (
          <div className="text-right font-mono">
            {formatQuantity(row.original.quantity)}
          </div>
        ),
      },
      {
        accessorKey: "unit_price",
        header: ({ column }) => (
          <SortHeader column={column} className="justify-end">
            Preço
          </SortHeader>
        ),
        cell: ({ row }) => (
          <div className="text-right font-mono">
            {row.original.unit_price != null
              ? formatCurrency(row.original.unit_price)
              : "\u2014"}
          </div>
        ),
      },
      {
        accessorKey: "total_value",
        header: ({ column }) => (
          <SortHeader column={column} className="justify-end">
            Valor
          </SortHeader>
        ),
        cell: ({ row }) => (
          <div className="text-right font-mono">
            {row.original.total_value != null
              ? formatCurrency(Math.abs(row.original.total_value))
              : "\u2014"}
          </div>
        ),
      },
      {
        accessorKey: "broker",
        header: ({ column }) => (
          <SortHeader column={column}>Corretora</SortHeader>
        ),
        cell: ({ row }) => (
          <span className="text-sm">{row.original.broker || "\u2014"}</span>
        ),
      },
    ],
    []
  );

  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
    manualSorting: true,
    state: { sorting },
    onSortingChange: handleSortingChange,
  });

  const totalPages = Math.ceil(total / perPage);
  const hasFilters =
    companyId !== null ||
    assetTypes.length > 0 ||
    !!operationType ||
    !!insiderGroup ||
    !!dateFrom ||
    !!dateTo ||
    !!valueMin ||
    !!valueMax;

  return (
    <div className="container mx-auto px-4 py-8 space-y-6">
      <h1 className="text-2xl font-bold">Movimentações</h1>

      {/* Filter Panel */}
      <div className="space-y-4 p-4 border rounded-lg bg-card">
        {/* Row 1: Company autocomplete + Operation type */}
        <div className="flex flex-col sm:flex-row gap-3">
          <div className="relative flex-1" ref={autocompleteRef}>
            {companyId ? (
              <div className="flex items-center gap-2 h-9 px-3 border rounded-md bg-muted/50">
                <span className="text-sm truncate">{companyName}</span>
                <button onClick={clearCompany} className="ml-auto shrink-0">
                  <X className="h-3.5 w-3.5 text-muted-foreground hover:text-foreground" />
                </button>
              </div>
            ) : (
              <>
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Buscar empresa..."
                  value={companySearch}
                  onChange={(e) => handleCompanySearch(e.target.value)}
                  onFocus={() =>
                    companySuggestions.length > 0 && setShowSuggestions(true)
                  }
                  className="pl-9"
                />
                {showSuggestions && companySuggestions.length > 0 && (
                  <div className="absolute z-50 top-full mt-1 w-full bg-popover border rounded-md shadow-md max-h-60 overflow-auto">
                    {companySuggestions.map((c) => (
                      <button
                        key={c.id}
                        className="w-full text-left px-3 py-2 text-sm hover:bg-accent flex items-center justify-between"
                        onClick={() => selectCompany(c)}
                      >
                        <span>{c.name}</span>
                        <span className="font-mono text-muted-foreground text-xs">
                          {displayTicker(c.ticker)}
                        </span>
                      </button>
                    ))}
                  </div>
                )}
              </>
            )}
          </div>

          <Select
            value={operationType || "all"}
            onValueChange={(v) => setOperationType(v === "all" ? "" : v)}
          >
            <SelectTrigger className="w-full sm:w-[160px]">
              <SelectValue placeholder="Operação" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todas</SelectItem>
              <SelectItem value="Compra">Compra</SelectItem>
              <SelectItem value="Venda">Venda</SelectItem>
            </SelectContent>
          </Select>

          <Select
            value={insiderGroup || "all"}
            onValueChange={(v) => setInsiderGroup(v === "all" ? "" : v)}
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

        {/* Row 2: Asset type multi-select */}
        <div>
          <p className="text-sm text-muted-foreground mb-2">Tipo de ativo</p>
          <div className="flex flex-wrap gap-2">
            {ASSET_TYPES.map((t) => (
              <Badge
                key={t.value}
                variant={assetTypes.includes(t.value) ? "default" : "outline"}
                className="cursor-pointer select-none"
                onClick={() => toggleAssetType(t.value)}
              >
                {t.label}
              </Badge>
            ))}
          </div>
        </div>

        {/* Row 3: Date range + Value range */}
        <div className="flex flex-col sm:flex-row gap-3">
          <div className="flex gap-2 items-center">
            <span className="text-sm text-muted-foreground whitespace-nowrap">
              De
            </span>
            <Input
              type="date"
              value={dateFrom}
              onChange={(e) => setDateFrom(e.target.value)}
              className="w-[160px]"
            />
            <span className="text-sm text-muted-foreground whitespace-nowrap">
              Até
            </span>
            <Input
              type="date"
              value={dateTo}
              onChange={(e) => setDateTo(e.target.value)}
              className="w-[160px]"
            />
          </div>
          <div className="flex gap-2 items-center">
            <span className="text-sm text-muted-foreground whitespace-nowrap">
              Valor min
            </span>
            <Input
              type="number"
              placeholder="0"
              value={valueMin}
              onChange={(e) => setValueMin(e.target.value)}
              className="w-[120px]"
            />
            <span className="text-sm text-muted-foreground whitespace-nowrap">
              max
            </span>
            <Input
              type="number"
              placeholder="0"
              value={valueMax}
              onChange={(e) => setValueMax(e.target.value)}
              className="w-[120px]"
            />
          </div>
        </div>

        {/* Row 4: Action buttons */}
        <div className="flex gap-3">
          <Button onClick={applyFilters}>
            <Filter className="mr-2 h-4 w-4" />
            Filtrar
          </Button>
          {hasFilters && (
            <Button variant="outline" onClick={clearFilters}>
              <X className="mr-2 h-4 w-4" />
              Limpar filtros
            </Button>
          )}
          <div className="ml-auto flex gap-2">
            <Button variant="outline" onClick={() => handleExport("csv")}>
              <Download className="mr-2 h-4 w-4" />
              CSV
            </Button>
            <Button variant="outline" onClick={() => handleExport("xlsx")}>
              <Download className="mr-2 h-4 w-4" />
              XLSX
            </Button>
          </div>
        </div>
      </div>

      {/* Results */}
      {loading ? (
        <div className="flex items-center justify-center py-16 text-muted-foreground">
          Carregando...
        </div>
      ) : data.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground space-y-2">
          <p className="text-lg">Nenhuma movimentação encontrada</p>
          <p className="text-sm">Tente ajustar os filtros de busca.</p>
        </div>
      ) : (
        <>
          {/* Total + per page */}
          <div className="flex items-center justify-between">
            <span className="text-sm text-muted-foreground">
              {formatQuantity(total)} movimentaç
              {total !== 1 ? "ões" : "ão"} encontrada
              {total !== 1 ? "s" : ""}
            </span>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">
                Por página:
              </span>
              <Select
                value={String(perPage)}
                onValueChange={(v) => handlePerPageChange(parseInt(v, 10))}
              >
                <SelectTrigger className="w-[80px] h-8">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {PER_PAGE_OPTIONS.map((n) => (
                    <SelectItem key={n} value={String(n)}>
                      {n}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Table */}
          <div className="border rounded-md overflow-x-auto">
            <Table>
              <TableHeader>
                {table.getHeaderGroups().map((hg) => (
                  <TableRow key={hg.id}>
                    {hg.headers.map((header) => (
                      <TableHead key={header.id}>
                        {header.isPlaceholder
                          ? null
                          : flexRender(
                              header.column.columnDef.header,
                              header.getContext()
                            )}
                      </TableHead>
                    ))}
                  </TableRow>
                ))}
              </TableHeader>
              <TableBody>
                {table.getRowModel().rows.map((row) => (
                  <TableRow key={row.id}>
                    {row.getVisibleCells().map((cell) => (
                      <TableCell key={cell.id}>
                        {flexRender(
                          cell.column.columnDef.cell,
                          cell.getContext()
                        )}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-center gap-2 pt-2">
              <Button
                variant="outline"
                size="sm"
                disabled={page <= 1}
                onClick={() => handlePageChange(page - 1)}
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
                onClick={() => handlePageChange(page + 1)}
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

export default function MovimentaçõesPage() {
  return (
    <Suspense
      fallback={
        <div className="container mx-auto px-4 py-8">
          <h1 className="text-2xl font-bold mb-6">Movimentações</h1>
          <div className="flex items-center justify-center py-16 text-muted-foreground">
            Carregando...
          </div>
        </div>
      }
    >
      <MovimentaçõesContent />
    </Suspense>
  );
}
