import type { Metadata } from "next";
import localFont from "next/font/local";
import { Header } from "@/components/header";
import "./globals.css";

const inter = localFont({
  src: "../../public/fonts/inter-latin-standard-normal.woff2",
  variable: "--font-inter",
  display: "swap",
});

const jetbrainsMono = localFont({
  src: "../../public/fonts/jetbrains-mono-latin-wght-normal.woff2",
  variable: "--font-jetbrains-mono",
  display: "swap",
});

export const metadata: Metadata = {
  title: "InsiderTrack - Movimentações de Insiders",
  description:
    "Acompanhe as movimentações de insiders em empresas brasileiras listadas na CVM",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="pt-BR"
      className={`${inter.variable} ${jetbrainsMono.variable} h-full antialiased`}
    >
      <body className="min-h-full flex flex-col">
        <Header />
        <main className="flex-1">{children}</main>
      </body>
    </html>
  );
}
