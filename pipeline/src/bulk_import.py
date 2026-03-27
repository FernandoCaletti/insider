"""Bulk import of CVM insider trading documents.

Parallel downloads + batched DB operations for fast historical import.
Each thread gets its own HTTP session (urllib is not thread-safe).

Usage:
    DATABASE_URL=postgresql://... python -m pipeline.src.bulk_import --years 2023 2024 2025
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

from pipeline.src.collector.cvm_client import (
    DocumentRecord,
    fetch_and_parse_documents,
)
from pipeline.src.collector.downloader import (
    cleanup_file,
    create_session,
    download_pdf,
    warmup_session,
)
from pipeline.src.config import get_settings
from pipeline.src.extractor.pdf_parser import extract_pdf
from pipeline.src.utils.hashing import sha256_hash

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger("bulk")

# Suppress noisy sub-loggers
for name in [
    "pipeline.src.loader.supabase_loader",
    "pipeline.src.collector.downloader",
    "pipeline.src.collector.cvm_client",
    "pipeline.src.extractor.pdf_parser",
]:
    logging.getLogger(name).setLevel(logging.WARNING)

WORKERS = 6

# Thread-local storage for HTTP sessions
_thread_local = threading.local()


def _get_session():
    """Get or create an HTTP session for the current thread."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = create_session()
        warmup_session(_thread_local.session)
    return _thread_local.session


def _decimal_to_float(val) -> float | None:
    if val is None:
        return None
    return float(val)


def load_existing_hashes(db_url: str) -> set[str]:
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT file_hash FROM documents")
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def load_company_map(db_url: str) -> dict[str, int]:
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT cvm_code, id FROM companies")
            return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()


@dataclass
class ProcessedDoc:
    doc: DocumentRecord
    doc_id: int | None = None
    file_hash: str = ""
    holdings: list = field(default_factory=list)
    page_count: int = 0
    is_scanned: bool = False
    error: str | None = None
    skipped: bool = False


def download_and_extract(doc: DocumentRecord) -> ProcessedDoc:
    """Download PDF and extract holdings (runs in thread with own session)."""
    result = ProcessedDoc(doc=doc)
    tmp_path = None
    try:
        session = _get_session()
        tmp_path = download_pdf(session, doc.document_url, max_retries=2, timeout=20)
        result.file_hash = sha256_hash(tmp_path)
        extraction = extract_pdf(tmp_path)
        result.page_count = extraction.page_count
        result.is_scanned = extraction.is_scanned
        if not extraction.is_scanned:
            result.holdings = extraction.all_holdings
        else:
            result.error = "scanned"
    except Exception as e:
        result.error = str(e)[:120]
    finally:
        if tmp_path:
            cleanup_file(tmp_path)
    return result


def save_batch(
    db_url: str,
    results: list[ProcessedDoc],
    company_map: dict[str, int],
    existing_hashes: set[str],
) -> tuple[int, int]:
    """Save processed documents to DB. Returns (saved, skipped)."""
    conn = psycopg2.connect(db_url)
    saved = 0
    skipped = 0
    try:
        with conn.cursor() as cur:
            for r in results:
                if r.error:
                    continue
                if r.file_hash in existing_hashes:
                    r.skipped = True
                    skipped += 1
                    continue

                company_id = company_map.get(r.doc.cvm_code)
                if not company_id:
                    r.error = f"no company {r.doc.cvm_code}"
                    continue

                try:
                    year = int(r.doc.reference_date[:4])
                    month = int(r.doc.reference_date[5:7])
                except (ValueError, IndexError):
                    year = datetime.now(tz=timezone.utc).year
                    month = 1

                try:
                    cur.execute("SAVEPOINT doc_save")
                    cur.execute(
                        """INSERT INTO documents
                            (company_id, reference_date, year, month, file_name,
                             file_hash, original_url, page_count, is_scanned)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (file_hash) DO UPDATE SET
                            page_count = EXCLUDED.page_count,
                            is_scanned = EXCLUDED.is_scanned,
                            processed_at = NOW()
                        RETURNING id""",
                        (company_id, r.doc.reference_date, year, month,
                         r.doc.document_url.split("/")[-1] if r.doc.document_url else None,
                         r.file_hash, r.doc.document_url, r.page_count, r.is_scanned),
                    )
                    row = cur.fetchone()
                    if not row:
                        cur.execute("ROLLBACK TO SAVEPOINT doc_save")
                        continue
                    r.doc_id = row[0]
                    cur.execute("RELEASE SAVEPOINT doc_save")
                except psycopg2.Error as e:
                    cur.execute("ROLLBACK TO SAVEPOINT doc_save")
                    r.error = f"db: {str(e)[:60]}"
                    continue

                if r.holdings and r.doc_id:
                    cur.execute("DELETE FROM holdings WHERE document_id = %s", (r.doc_id,))
                    params = [
                        (r.doc_id, h.section, h.asset_type, h.asset_description,
                         _decimal_to_float(h.quantity), _decimal_to_float(h.unit_price),
                         _decimal_to_float(h.total_value), h.operation_type,
                         h.operation_date, h.broker, h.confidence, h.insider_group)
                        for h in r.holdings
                    ]
                    psycopg2.extras.execute_batch(
                        cur,
                        """INSERT INTO holdings
                            (document_id, section, asset_type, asset_description, quantity,
                             unit_price, total_value, operation_type, operation_date, broker, confidence, insider_group)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        params,
                        page_size=500,
                    )

                existing_hashes.add(r.file_hash)
                saved += 1

        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Error saving batch")
    finally:
        conn.close()

    return saved, skipped


def run(years: list[int]):
    settings = get_settings()
    db_url = settings.database_url

    log.info("Loading existing data...")
    existing_hashes = load_existing_hashes(db_url)
    company_map = load_company_map(db_url)
    log.info("Loaded %d existing hashes, %d companies", len(existing_hashes), len(company_map))

    raw_docs: list[DocumentRecord] = []
    for year in years:
        log.info("Fetching document list for %d...", year)
        docs = fetch_and_parse_documents(year, settings.cvm_base_url)
        log.info("  %d: %d documents", year, len(docs))
        raw_docs.extend(docs)

    # Filter: keep only "Posição Consolidada" (has insider movements).
    # "Posição Individual" only has company-level positions, no insider trades.
    consolidated = [d for d in raw_docs if "consolidada" in d.document_type.lower()]
    log.info("Filtered to %d Consolidada (from %d total)", len(consolidated), len(raw_docs))

    # Deduplicate: when multiple versions exist for the same (cvm_code, reference_date),
    # keep only the highest version (latest resubmission).
    from collections import defaultdict
    by_key: dict[tuple[str, str], DocumentRecord] = {}
    for d in consolidated:
        key = (d.cvm_code, d.reference_date)
        existing = by_key.get(key)
        if existing is None or int(d.version or "0") > int(existing.version or "0"):
            by_key[key] = d

    all_docs = list(by_key.values())
    log.info("After version dedup: %d documents with %d workers", len(all_docs), WORKERS)

    total_saved = 0
    total_skipped = 0
    total_errors = 0
    done_count = 0
    batch: list[ProcessedDoc] = []
    BATCH_SIZE = 50
    start_time = time.time()

    def flush():
        nonlocal total_saved, total_skipped, total_errors
        if not batch:
            return
        s, sk = save_batch(db_url, batch, company_map, existing_hashes)
        errs = sum(1 for r in batch if r.error)
        total_saved += s
        total_skipped += sk
        total_errors += errs
        elapsed = time.time() - start_time
        rate = (total_saved + total_skipped + total_errors) / elapsed if elapsed > 0 else 0
        eta_min = (len(all_docs) - done_count) / rate / 60 if rate > 0 else 0
        log.info(
            "saved=%d skip=%d err=%d [%d/%d] %.1f/s ETA %.0fm",
            total_saved, total_skipped, total_errors,
            done_count, len(all_docs), rate, eta_min,
        )
        batch.clear()

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        # Submit all work — ThreadPoolExecutor queues excess tasks internally
        futures = {
            executor.submit(download_and_extract, doc): doc
            for doc in all_docs
        }

        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                result = ProcessedDoc(doc=futures[future], error=str(e)[:120])

            batch.append(result)
            done_count += 1

            if len(batch) >= BATCH_SIZE:
                flush()

    flush()

    elapsed = time.time() - start_time
    log.info(
        "DONE in %.0fs (%.1f min): saved=%d skipped=%d errors=%d total=%d",
        elapsed, elapsed / 60, total_saved, total_skipped, total_errors, len(all_docs),
    )


def main():
    parser = argparse.ArgumentParser(description="Bulk import CVM documents")
    parser.add_argument("--years", type=int, nargs="+", default=[2023, 2024, 2025])
    args = parser.parse_args()
    run(args.years)


if __name__ == "__main__":
    main()
