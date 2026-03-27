/**
 * Format a number as Brazilian currency (R$).
 * Example: 1925000 -> "R$ 1.925.000,00"
 */
export function formatCurrency(value: number): string {
  return value.toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
  });
}

/**
 * Format a date string or Date object as dd/mm/yyyy.
 * Example: "2026-03-20" -> "20/03/2026"
 */
export function formatDate(date: string | Date): string {
  const d = typeof date === "string" ? new Date(date + "T00:00:00") : date;
  return d.toLocaleDateString("pt-BR", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  });
}

/**
 * Format a number with Brazilian thousands separators.
 * Example: 50000 -> "50.000"
 */
export function formatQuantity(value: number): string {
  return value.toLocaleString("pt-BR", {
    maximumFractionDigits: 0,
  });
}

/**
 * Display a ticker value, showing a dash for null/undefined.
 */
export function displayTicker(ticker: string | null | undefined): string {
  return ticker || "\u2014";
}
