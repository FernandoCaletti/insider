"""Historical backfill script for CVM insider trading PDFs.

Supports two modes:

1. **Local mode** – process PDFs already on disk, organised as
   ``{source}/{cvm_code}/{year}/*.pdf``.
2. **Download mode** – fetch the CVM document catalogue for one or more
   years, download each PDF, and process it.

Both modes are idempotent: a SHA-256 file hash is checked before work
begins.  Pass ``--force`` to reprocess documents whose hash is already in
the database.

Usage examples::

    # Local files
    python -m pipeline.src.backfill --source downloads/
    python -m pipeline.src.backfill --source downloads/ --company 9512
    python -m pipeline.src.backfill --source downloads/ --year 2024
    python -m pipeline.src.backfill --source downloads/ --force

    # Download from CVM
    python -m pipeline.src.backfill --download --years 2023 2024 2025
    python -m pipeline.src.backfill --download --years 2024 --company 9512
    python -m pipeline.src.backfill --download --years 2024 --force
    python -m pipeline.src.backfill --download --years 2024 --workers 8
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
from pipeline.src.extractor.pdf_parser import ExtractionResult, extract_pdf
from pipeline.src.loader.supabase_loader import (
    file_hash_exists,
    get_company_id,
    upsert_document,
    upsert_holdings,
)
from pipeline.src.utils.hashing import sha256_hash

# ---------------------------------------------------------------------------
# Structured JSON logging
# ---------------------------------------------------------------------------

_FORM_PERIOD_RE = re.compile(r"Em\s+(\d{1,2})/(\d{4})", re.IGNORECASE)


class _JSONFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1]:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


def setup_logging(level: str = "INFO") -> None:
    """Configure structured JSON logging to stdout."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JSONFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _infer_month_from_result(result: ExtractionResult, pdf_path: str) -> int:
    """Try to determine the filing month from the extraction result.

    Strategy (first match wins):
    1. Most common month among holdings with operation_date set.
    2. "Em MM/YYYY" regex on the raw first-page text.
    3. Fallback to 1.
    """
    # 1. From holdings operation_dates
    month_counts: dict[int, int] = defaultdict(int)
    for h in result.all_holdings:
        if h.operation_date:
            try:
                month_counts[int(h.operation_date[5:7])] += 1
            except (ValueError, IndexError):
                pass
    if month_counts:
        return max(month_counts, key=lambda m: month_counts[m])

    # 2. Scan PDF text for "Em MM/YYYY"
    try:
        import pdfplumber  # type: ignore[import-untyped]

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages[:3]:
                text = page.extract_text() or ""
                match = _FORM_PERIOD_RE.search(text)
                if match:
                    return int(match.group(1))
    except Exception:
        pass

    return 1


# ---------------------------------------------------------------------------
# PDF discovery (local mode)
# ---------------------------------------------------------------------------


def discover_pdfs(
    source_dir: Path,
    company_filter: str | None = None,
    year_filter: int | None = None,
) -> list[tuple[str, int, Path]]:
    """Find PDFs in ``{source}/{cvm_code}/{year}/*.pdf``.

    Returns:
        List of (cvm_code, year, pdf_path) tuples, sorted for deterministic order.
    """
    results: list[tuple[str, int, Path]] = []

    if not source_dir.is_dir():
        return results

    for cvm_dir in sorted(source_dir.iterdir()):
        if not cvm_dir.is_dir():
            continue
        cvm_code = cvm_dir.name
        if company_filter and cvm_code != company_filter:
            continue

        for year_dir in sorted(cvm_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            try:
                year = int(year_dir.name)
            except ValueError:
                continue
            if year_filter and year != year_filter:
                continue

            for pdf_file in sorted(year_dir.glob("*.pdf")):
                results.append((cvm_code, year, pdf_file))

    return results


# ---------------------------------------------------------------------------
# Local backfill
# ---------------------------------------------------------------------------


def run_local_backfill(
    source_dir: Path,
    company_filter: str | None = None,
    year_filter: int | None = None,
    force: bool = False,
) -> None:
    """Process PDFs from a local directory structure."""
    settings = get_settings()
    setup_logging(settings.log_level)

    logger.info(
        "Starting local backfill source=%s company=%s year=%s force=%s",
        source_dir,
        company_filter or "all",
        year_filter or "all",
        force,
    )

    pdfs = discover_pdfs(source_dir, company_filter, year_filter)
    logger.info("Found %d PDF files to process", len(pdfs))

    processed = 0
    skipped = 0
    failed = 0

    for cvm_code, year, pdf_path in pdfs:
        try:
            file_hash = sha256_hash(pdf_path)
            if not force and file_hash_exists(settings.database_url, file_hash):
                logger.debug(
                    "Already processed %s (hash=%s), skipping",
                    pdf_path.name,
                    file_hash[:12],
                )
                skipped += 1
                continue

            company_id = get_company_id(settings.database_url, cvm_code)
            if company_id is None:
                logger.warning(
                    "Company not found cvm_code=%s, skipping %s",
                    cvm_code,
                    pdf_path.name,
                )
                failed += 1
                continue

            result = extract_pdf(str(pdf_path))
            if result.is_scanned:
                logger.warning("Scanned PDF skipped: %s", pdf_path.name)
                failed += 1
                continue

            month = _infer_month_from_result(result, str(pdf_path))
            ref_date = f"{year}-{month:02d}-01"

            doc_id = upsert_document(
                database_url=settings.database_url,
                company_id=company_id,
                reference_date=ref_date,
                year=year,
                month=month,
                file_name=pdf_path.name,
                file_hash=file_hash,
                original_url=None,
                page_count=result.page_count,
                is_scanned=result.is_scanned,
            )
            if doc_id is None:
                failed += 1
                continue

            all_holdings = result.all_holdings
            if all_holdings:
                upsert_holdings(
                    settings.database_url, doc_id, all_holdings, batch_size=100
                )

            processed += 1
            logger.info(
                "Backfilled %s cvm=%s year=%d month=%d holdings=%d",
                pdf_path.name,
                cvm_code,
                year,
                month,
                len(all_holdings),
            )

        except Exception:
            failed += 1
            logger.exception("Error processing %s", pdf_path)

    logger.info(
        "Local backfill complete: processed=%d skipped=%d failed=%d total=%d",
        processed,
        skipped,
        failed,
        len(pdfs),
    )


# ---------------------------------------------------------------------------
# Download backfill (from CVM)
# ---------------------------------------------------------------------------

_thread_local = threading.local()


def _get_session() -> Any:
    """Get or create a thread-local HTTP session."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = create_session()
        warmup_session(_thread_local.session)
    return _thread_local.session


@dataclass
class _DownloadResult:
    """Result of downloading and extracting a single CVM document."""

    doc: DocumentRecord
    file_hash: str = ""
    tmp_path: str = ""
    extraction: ExtractionResult | None = None
    error: str | None = None


def _download_and_extract(doc: DocumentRecord) -> _DownloadResult:
    """Download a PDF and extract holdings (runs in worker thread)."""
    result = _DownloadResult(doc=doc)
    tmp_path: str | None = None
    try:
        session = _get_session()
        tmp_path = download_pdf(session, doc.document_url, max_retries=2, timeout=20)
        result.tmp_path = tmp_path
        result.file_hash = sha256_hash(tmp_path)
        result.extraction = extract_pdf(tmp_path)
    except Exception as e:
        result.error = str(e)[:200]
    finally:
        if tmp_path:
            cleanup_file(tmp_path)
    return result


def _deduplicate_documents(
    docs: list[DocumentRecord],
) -> list[DocumentRecord]:
    """Keep only the highest-version document per (cvm_code, reference_date)."""
    by_key: dict[tuple[str, str], DocumentRecord] = {}
    for d in docs:
        key = (d.cvm_code, d.reference_date)
        existing = by_key.get(key)
        if existing is None or int(d.version or "0") > int(existing.version or "0"):
            by_key[key] = d
    return list(by_key.values())


def run_download_backfill(
    years: list[int],
    company_filter: str | None = None,
    force: bool = False,
    workers: int = 4,
) -> None:
    """Download documents from CVM and process them."""
    settings = get_settings()
    setup_logging(settings.log_level)

    logger.info(
        "Starting download backfill years=%s company=%s force=%s workers=%d",
        years,
        company_filter or "all",
        force,
        workers,
    )

    # Fetch document listings from CVM
    raw_docs: list[DocumentRecord] = []
    for year in years:
        logger.info("Fetching CVM document catalogue for %d ...", year)
        docs = fetch_and_parse_documents(year, settings.cvm_base_url)
        logger.info("  year %d: %d documents", year, len(docs))
        raw_docs.extend(docs)

    # Filter to Consolidada (insider trading forms) only
    consolidated = [
        d for d in raw_docs if "consolidada" in d.document_type.lower()
    ]
    logger.info(
        "Filtered to %d Consolidada (from %d total)", len(consolidated), len(raw_docs)
    )

    # Optional company filter
    if company_filter:
        consolidated = [d for d in consolidated if d.cvm_code == company_filter]
        logger.info("Company filter applied: %d documents", len(consolidated))

    # Version deduplication
    all_docs = _deduplicate_documents(consolidated)
    logger.info("After version dedup: %d documents", len(all_docs))

    if not all_docs:
        logger.info("No documents to process")
        return

    # Pre-load existing hashes and company map for fast lookups
    import psycopg2

    conn = psycopg2.connect(settings.database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT file_hash FROM documents")
            existing_hashes: set[str] = {row[0] for row in cur.fetchall()}
            cur.execute("SELECT cvm_code, id FROM companies")
            company_map: dict[str, int] = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()

    logger.info(
        "Loaded %d existing hashes, %d companies",
        len(existing_hashes),
        len(company_map),
    )

    processed = 0
    skipped = 0
    failed = 0
    done_count = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_download_and_extract, doc): doc for doc in all_docs
        }

        for future in as_completed(futures):
            done_count += 1
            try:
                r = future.result()
            except Exception as e:
                failed += 1
                logger.error("Worker error: %s", e)
                continue

            if r.error:
                failed += 1
                logger.warning(
                    "Download/extract failed for %s: %s",
                    r.doc.document_url,
                    r.error,
                )
                continue

            # Hash check
            if not force and r.file_hash in existing_hashes:
                skipped += 1
                continue

            extraction = r.extraction
            if extraction is None:
                failed += 1
                continue

            if extraction.is_scanned:
                failed += 1
                logger.debug("Scanned PDF skipped: %s", r.doc.document_url)
                continue

            company_id = company_map.get(r.doc.cvm_code)
            if company_id is None:
                failed += 1
                logger.warning("Company not found cvm_code=%s", r.doc.cvm_code)
                continue

            # Parse reference_date from CVM catalogue
            try:
                year = int(r.doc.reference_date[:4])
                month = int(r.doc.reference_date[5:7])
            except (ValueError, IndexError):
                year = datetime.now(tz=timezone.utc).year
                month = 1

            ref_date = r.doc.reference_date or f"{year}-{month:02d}-01"
            file_name = r.doc.document_url.split("/")[-1] if r.doc.document_url else None

            doc_id = upsert_document(
                database_url=settings.database_url,
                company_id=company_id,
                reference_date=ref_date,
                year=year,
                month=month,
                file_name=file_name,
                file_hash=r.file_hash,
                original_url=r.doc.document_url,
                page_count=extraction.page_count,
                is_scanned=extraction.is_scanned,
            )
            if doc_id is None:
                failed += 1
                continue

            all_holdings = extraction.all_holdings
            if all_holdings:
                upsert_holdings(
                    settings.database_url, doc_id, all_holdings, batch_size=100
                )

            existing_hashes.add(r.file_hash)
            processed += 1

            # Progress reporting every 50 docs
            if done_count % 50 == 0:
                elapsed = time.time() - start_time
                rate = done_count / elapsed if elapsed > 0 else 0
                eta_min = (len(all_docs) - done_count) / rate / 60 if rate > 0 else 0
                logger.info(
                    "Progress: processed=%d skipped=%d failed=%d [%d/%d] %.1f/s ETA %.0fm",
                    processed,
                    skipped,
                    failed,
                    done_count,
                    len(all_docs),
                    rate,
                    eta_min,
                )

    elapsed = time.time() - start_time
    logger.info(
        "Download backfill complete in %.0fs: processed=%d skipped=%d failed=%d total=%d",
        elapsed,
        processed,
        skipped,
        failed,
        len(all_docs),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Backfill CVM insider trading PDFs (local or download mode)",
    )

    # Mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--source",
        type=str,
        default=None,
        help="Local mode: root directory with {cvm_code}/{year}/*.pdf",
    )
    mode_group.add_argument(
        "--download",
        action="store_true",
        default=False,
        help="Download mode: fetch documents from CVM",
    )

    # Shared filters
    parser.add_argument(
        "--company",
        type=str,
        default=None,
        help="Only process this CVM code",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Reprocess even if file_hash already exists",
    )

    # Local mode options
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Local mode: only process this year",
    )

    # Download mode options
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help="Download mode: years to fetch (e.g. --years 2023 2024 2025)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Download mode: number of parallel workers (default 4)",
    )

    args = parser.parse_args(argv)

    # Validation
    if args.download and not args.years:
        parser.error("--years is required with --download")

    return args


def main() -> None:
    """Entry point for ``python -m pipeline.src.backfill``."""
    args = parse_args()

    if args.source:
        run_local_backfill(
            source_dir=Path(args.source),
            company_filter=args.company,
            year_filter=args.year,
            force=args.force,
        )
    else:
        run_download_backfill(
            years=args.years or [],
            company_filter=args.company,
            force=args.force,
            workers=args.workers,
        )


if __name__ == "__main__":
    main()
