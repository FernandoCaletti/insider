"""Historical backfill script.

Traverses a directory of downloaded PDFs organised as
``{source}/{cvm_code}/{year}/*.pdf``, checks file_hash before reprocessing
(idempotent), and loads data using the same pdf_parser.py as the cron job.

Usage:
    python -m pipeline.src.backfill --source downloads/
    python -m pipeline.src.backfill --source downloads/ --company 9512
    python -m pipeline.src.backfill --source downloads/ --year 2024
    python -m pipeline.src.backfill --source downloads/ --company 9512 --year 2024
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.src.config import get_settings
from pipeline.src.extractor.pdf_parser import extract_pdf
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


# ---------------------------------------------------------------------------
# PDF discovery
# ---------------------------------------------------------------------------


def discover_pdfs(
    source_dir: Path,
    company_filter: str | None = None,
    year_filter: int | None = None,
) -> list[tuple[str, int, Path]]:
    """Find PDFs in the directory structure {source}/{cvm_code}/{year}/*.pdf.

    Args:
        source_dir: Root directory containing company folders.
        company_filter: If set, only process this cvm_code.
        year_filter: If set, only process this year.

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
# Backfill processing
# ---------------------------------------------------------------------------


def run_backfill(
    source_dir: Path,
    company_filter: str | None = None,
    year_filter: int | None = None,
) -> None:
    """Run the historical backfill over local PDF files."""
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = logging.getLogger(__name__)

    logger.info(
        "Starting backfill source=%s company=%s year=%s",
        source_dir,
        company_filter or "all",
        year_filter or "all",
    )

    pdfs = discover_pdfs(source_dir, company_filter, year_filter)
    logger.info("Found %d PDF files to process", len(pdfs))

    processed = 0
    skipped = 0
    failed = 0

    for cvm_code, year, pdf_path in pdfs:
        try:
            # Hash check (idempotent)
            file_hash = sha256_hash(pdf_path)
            if file_hash_exists(settings.database_url, file_hash):
                logger.debug("Already processed %s (hash=%s), skipping", pdf_path.name, file_hash[:12])
                skipped += 1
                continue

            # Look up company
            company_id = get_company_id(settings.database_url, cvm_code)
            if company_id is None:
                logger.warning(
                    "Company not found for cvm_code=%s, skipping %s",
                    cvm_code,
                    pdf_path.name,
                )
                failed += 1
                continue

            # Extract
            result = extract_pdf(str(pdf_path))
            if result.is_scanned:
                logger.warning("Scanned PDF skipped: %s", pdf_path.name)
                failed += 1
                continue

            # Build reference date from year dir and filename or default
            # Try to infer month from the PDF or default to 1
            month = 1
            ref_date = f"{year}-{month:02d}-01"

            # Upsert document
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

            # Upsert holdings
            all_holdings = result.all_holdings
            if all_holdings:
                upsert_holdings(
                    settings.database_url, doc_id, all_holdings, batch_size=100
                )

            processed += 1
            logger.info(
                "Backfilled %s cvm=%s year=%d holdings=%d",
                pdf_path.name,
                cvm_code,
                year,
                len(all_holdings),
            )

        except Exception:
            failed += 1
            logger.exception("Error processing %s", pdf_path)

    logger.info(
        "Backfill complete: processed=%d skipped=%d failed=%d total=%d",
        processed,
        skipped,
        failed,
        len(pdfs),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Historical backfill of CVM insider trading PDFs",
    )
    parser.add_argument(
        "--source",
        type=str,
        required=True,
        help="Root directory with structure {cvm_code}/{year}/*.pdf",
    )
    parser.add_argument(
        "--company",
        type=str,
        default=None,
        help="Only process this CVM code",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Only process this year",
    )
    return parser.parse_args(argv)


def main() -> None:
    """Entry point for python -m pipeline.src.backfill."""
    args = parse_args()
    run_backfill(
        source_dir=Path(args.source),
        company_filter=args.company,
        year_filter=args.year,
    )


if __name__ == "__main__":
    main()
