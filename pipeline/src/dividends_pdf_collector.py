"""Dividends collector — fetch and parse CVM 'Relatório Proventos' PDFs.

Downloads the IPE CSV, filters for 'Relatório Proventos' entries,
downloads each PDF, extracts dividend data, and upserts into the
dividends table.

Usage:
    python -m pipeline.src.dividends_pdf_collector
    python -m pipeline.src.dividends_pdf_collector --years 2024 2025
    python -m pipeline.src.dividends_pdf_collector --years 2025 --workers 4
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import pdfplumber
import psycopg2
import psycopg2.extras

from pipeline.src.collector.cvm_client import fetch_document_zip
from pipeline.src.collector.downloader import (
    cleanup_file,
    create_session,
    download_pdf,
    warmup_session,
)
from pipeline.src.config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger("dividends")

WORKERS = 4
_thread_local = threading.local()

PROVENTOS_CATEGORY = "Relatório Proventos"


def _get_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = create_session()
        warmup_session(_thread_local.session)
    return _thread_local.session


# ---------------------------------------------------------------------------
# PDF parsing
# ---------------------------------------------------------------------------


@dataclass
class DividendEntry:
    cvm_code: str
    isin: str
    value_per_share: Decimal
    period: str
    fiscal_year: str
    payment_date: str | None  # YYYY-MM-DD
    ex_date: str | None  # YYYY-MM-DD (ultimo dia com direitos)
    approval_date: str | None  # YYYY-MM-DD
    dividend_type: str  # JCP, Dividendo, Rendimento
    payment_form: str | None


def _parse_date(raw: str | None) -> str | None:
    if not raw or not raw.strip():
        return None
    text = raw.strip()
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", text)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    if re.match(r"\d{4}-\d{2}-\d{2}$", text):
        return text
    return None


def _parse_value(raw: str | None) -> Decimal | None:
    if not raw or not raw.strip():
        return None
    text = raw.strip()
    # Remove line breaks and extra whitespace
    text = re.sub(r"\s+", "", text)
    # Brazilian: comma is decimal
    text = text.replace(".", "").replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _infer_dividend_type(isin: str, period: str) -> str:
    """Infer dividend type from ISIN and period description."""
    period_lower = period.lower() if period else ""
    if "jcp" in period_lower or "juros" in period_lower:
        return "JCP"
    if "rendimento" in period_lower:
        return "Rendimento"
    return "Dividendo"


def parse_proventos_pdf(pdf_path: str, cvm_code: str) -> list[DividendEntry]:
    """Parse a CVM Relatório Proventos PDF into dividend entries."""
    entries: list[DividendEntry] = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables() or []

                # Extract metadata from table 0
                ex_date = None
                approval_date = None
                for table in tables:
                    for row in table:
                        if not row:
                            continue
                        row_text = [str(c or "").strip() for c in row]
                        # Look for "Data Aprovação" / "Ultimo dia de negociação com Direitos"
                        if any("Data Aprov" in c for c in row_text):
                            # Next row has the dates
                            continue
                        if any("ltimo dia" in c for c in row_text):
                            continue

                        # Check if this row has dates in DD/MM/YYYY format
                        dates_in_row = [c for c in row_text if re.match(r"\d{2}/\d{2}/\d{4}$", c)]
                        if len(dates_in_row) == 2 and not approval_date:
                            approval_date = _parse_date(dates_in_row[0])
                            ex_date = _parse_date(dates_in_row[1])

                # Extract dividend data from table 1 (the main data table)
                for table in tables:
                    if len(table) < 2:
                        continue

                    # Check if this is the dividend data table
                    header_text = " ".join(str(c or "") for c in table[0] + (table[1] if len(table) > 1 else []))
                    if "ISIN" not in header_text and "Valor Bruto" not in header_text:
                        continue

                    # Find header row
                    header_idx = 0
                    for i, row in enumerate(table):
                        row_text = " ".join(str(c or "") for c in row)
                        if "ISIN" in row_text or "Valor Bruto" in row_text:
                            header_idx = i
                            break

                    # Parse data rows
                    for row in table[header_idx + 1:]:
                        if not row or len(row) < 3:
                            continue

                        cells = [str(c or "").strip() for c in row]

                        # ISIN is in first column (may have line breaks)
                        isin_raw = cells[0].replace("\n", "")
                        if not isin_raw or len(isin_raw) < 5:
                            continue

                        # Value per share in second column
                        value = _parse_value(cells[1] if len(cells) > 1 else None)
                        if value is None or value <= 0:
                            continue

                        # Period in third column
                        period = cells[2] if len(cells) > 2 else ""
                        fiscal_year = cells[3] if len(cells) > 3 else ""
                        payment_form = cells[5] if len(cells) > 5 else None
                        payment_date_raw = cells[6] if len(cells) > 6 else None
                        payment_date = _parse_date(payment_date_raw)

                        entries.append(DividendEntry(
                            cvm_code=cvm_code,
                            isin=isin_raw,
                            value_per_share=value,
                            period=f"{period} {fiscal_year}".strip(),
                            fiscal_year=fiscal_year,
                            payment_date=payment_date,
                            ex_date=ex_date,
                            approval_date=approval_date,
                            dividend_type=_infer_dividend_type(isin_raw, period),
                            payment_form=payment_form,
                        ))

    except Exception as e:
        log.warning("Error parsing proventos PDF %s: %s", pdf_path, e)

    return entries


# ---------------------------------------------------------------------------
# Download + parse (runs in thread)
# ---------------------------------------------------------------------------


@dataclass
class ProcessedProvento:
    cvm_code: str
    url: str
    entries: list[DividendEntry] = field(default_factory=list)
    error: str | None = None


def download_and_parse(cvm_code: str, url: str) -> ProcessedProvento:
    result = ProcessedProvento(cvm_code=cvm_code, url=url)
    tmp_path = None
    try:
        session = _get_session()
        tmp_path = download_pdf(session, url, max_retries=2, timeout=20)
        result.entries = parse_proventos_pdf(tmp_path, cvm_code)
    except Exception as e:
        result.error = str(e)[:120]
    finally:
        if tmp_path:
            cleanup_file(tmp_path)
    return result


# ---------------------------------------------------------------------------
# DB save
# ---------------------------------------------------------------------------


def save_dividends(
    db_url: str,
    entries: list[DividendEntry],
    company_map: dict[str, int],
) -> int:
    if not entries:
        return 0

    conn = psycopg2.connect(db_url)
    saved = 0
    try:
        with conn.cursor() as cur:
            for e in entries:
                company_id = company_map.get(e.cvm_code)
                if not company_id:
                    continue

                try:
                    cur.execute("SAVEPOINT div_save")
                    cur.execute(
                        """INSERT INTO dividends
                            (company_id, ex_date, payment_date, record_date,
                             dividend_type, value_per_share, currency)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (company_id, ex_date, dividend_type)
                        DO UPDATE SET
                            payment_date = EXCLUDED.payment_date,
                            value_per_share = EXCLUDED.value_per_share
                        """,
                        (
                            company_id,
                            e.ex_date or e.approval_date,
                            e.payment_date,
                            e.approval_date,
                            e.dividend_type,
                            float(e.value_per_share),
                            "BRL",
                        ),
                    )
                    cur.execute("RELEASE SAVEPOINT div_save")
                    saved += 1
                except psycopg2.Error:
                    cur.execute("ROLLBACK TO SAVEPOINT div_save")

        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Error saving dividends")
    finally:
        conn.close()

    return saved


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(years: list[int]):
    settings = get_settings()
    db_url = settings.database_url

    # Load company map
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT cvm_code, id FROM companies")
            company_map = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()

    log.info("Loaded %d companies", len(company_map))

    # Collect proventos docs from IPE CSV
    all_docs: list[tuple[str, str]] = []  # (cvm_code, url)
    for year in years:
        log.info("Fetching IPE CSV for %d...", year)
        content = fetch_document_zip(year, settings.cvm_base_url)
        reader = csv.DictReader(io.StringIO(content), delimiter=";")
        count = 0
        for row in reader:
            cat = row.get("Categoria", row.get("CATEG_DOC", ""))
            if PROVENTOS_CATEGORY not in cat:
                continue
            cvm_code = row.get("Codigo_CVM", row.get("CD_CVM", "")).strip()
            url = row.get("Link_Download", row.get("LINK_DOC", "")).strip()
            if cvm_code and url:
                all_docs.append((cvm_code, url))
                count += 1
        log.info("  %d: %d proventos docs", year, count)

    log.info("Total: %d proventos PDFs to process", len(all_docs))

    total_saved = 0
    total_entries = 0
    total_errors = 0
    done = 0
    start = time.time()
    batch_entries: list[DividendEntry] = []

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(download_and_parse, cvm, url): (cvm, url)
            for cvm, url in all_docs
        }

        for future in as_completed(futures):
            result = future.result()
            done += 1

            if result.error:
                total_errors += 1
            else:
                batch_entries.extend(result.entries)
                total_entries += len(result.entries)

            if len(batch_entries) >= 100 or done == len(all_docs):
                saved = save_dividends(db_url, batch_entries, company_map)
                total_saved += saved
                batch_entries.clear()

                if done % 50 == 0 or done == len(all_docs):
                    elapsed = time.time() - start
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (len(all_docs) - done) / rate / 60 if rate > 0 else 0
                    log.info(
                        "Progress: %d/%d saved=%d entries=%d errors=%d (%.1f/s ETA %.0fm)",
                        done, len(all_docs), total_saved, total_entries, total_errors, rate, eta,
                    )

    log.info(
        "DONE: saved=%d entries=%d errors=%d total_pdfs=%d",
        total_saved, total_entries, total_errors, len(all_docs),
    )


def main():
    parser = argparse.ArgumentParser(description="Collect dividends from CVM proventos PDFs")
    current_year = datetime.now(tz=timezone.utc).year
    parser.add_argument("--years", type=int, nargs="+", default=[current_year])
    parser.add_argument("--workers", type=int, default=WORKERS)
    args = parser.parse_args()
    run(args.years)


if __name__ == "__main__":
    main()
