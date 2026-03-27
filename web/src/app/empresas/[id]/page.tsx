import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { api, ApiError } from "@/lib/api";
import type { CompanyDetail } from "@/lib/types";
import { CompanyDetailClient } from "@/components/company/company-detail";

interface Props {
  params: Promise<{ id: string }>;
}

async function getCompany(id: string): Promise<CompanyDetail | null> {
  try {
    const result = await api.get<{ data: CompanyDetail }>(`/companies/${id}`, {
      next: { revalidate: 3600 },
    });
    return result.data;
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) {
      return null;
    }
    throw err;
  }
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { id } = await params;
  const company = await getCompany(id);
  if (!company) {
    return { title: "Empresa não encontrada | InsiderTrack" };
  }
  const ticker = company.ticker ? ` (${company.ticker})` : "";
  return {
    title: `${company.name}${ticker} - Movimentações de Insiders | InsiderTrack`,
  };
}

export default async function CompanyDetailPage({ params }: Props) {
  const { id } = await params;
  const company = await getCompany(id);
  if (!company) {
    notFound();
  }
  return <CompanyDetailClient company={company} />;
}
