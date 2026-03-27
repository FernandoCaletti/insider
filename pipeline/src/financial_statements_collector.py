"""Financial statements collector — fetch DFP/ITR data from CVM.

Downloads the CVM financial statement ZIPs (DFP for annual, ITR for quarterly)
for each requested year, parses statement CSVs (BPA, BPP, DRE, DFC_MI),
and upserts them into the financial_statements table.
Idempotent via the (company_id, reference_date, statement_type, account_code)
UNIQUE constraint.

Usage:
    python -m pipeline.src.financial_statements_collector
    python -m pipeline.src.financial_statements_collector --years 2023 2024 2025
    python -m pipeline.src.financial_statements_collector --report-type ITR
    python -m pipeline.src.financial_statements_collector --report-type DFP --years 2024
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

import psycopg2

from pipeline.src.collector.cvm_client import (
    FinancialStatementRecord,
    fetch_and_parse_financial_statements,
)
from pipeline.src.config import get_settings
from pipeline.src.loader.supabase_loader import upsert_financial_statements


# ---------------------------------------------------------------------------
# Structured JSON logging (reused pattern from material_facts_collector.py)
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


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Company map loader
# ---------------------------------------------------------------------------


def _load_company_map(database_url: str) -> dict[str, int]:
    """Load cvm_code -> company id mapping from the database."""
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT cvm_code, id FROM companies")
            return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


def _deduplicate(
    records: list[FinancialStatementRecord],
) -> list[FinancialStatementRecord]:
    """Deduplicate by (cvm_code, reference_date, statement_type, account_code).

    Keeps the record with the highest version.
    """
    by_key: dict[tuple[str, str, str, str], FinancialStatementRecord] = {}
    for r in records:
        key = (r.cvm_code, r.reference_date, r.statement_type, r.account_code)
        existing = by_key.get(key)
        if existing is None or r.version > existing.version:
            by_key[key] = r
    return list(by_key.values())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Collect financial statements (DFP/ITR) from CVM.",
    )
    current_year = datetime.now(tz=timezone.utc).year
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=[current_year],
        help="Years to fetch (default: current year).",
    )
    parser.add_argument(
        "--report-type",
        choices=["DFP", "ITR", "both"],
        default="both",
        help="Report type: DFP (annual), ITR (quarterly), or both (default: both).",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(years: list[int], report_type: str = "both") -> None:
    """Fetch and load financial statements for the given years."""
    settings = get_settings()
    setup_logging(settings.log_level)

    report_types = ["DFP", "ITR"] if report_type == "both" else [report_type]

    logger.info(
        "Financial statements collector starting: years=%s report_types=%s",
        years,
        report_types,
    )

    # Pre-load company map
    company_map = _load_company_map(settings.database_url)
    logger.info("Loaded %d companies from database", len(company_map))

    total_fetched = 0
    total_upserted = 0

    for year in years:
        for rt in report_types:
            logger.info("Fetching %s financial statements for year %d", rt, year)
            try:
                records = fetch_and_parse_financial_statements(
                    year, rt, settings.cvm_base_url
                )
            except Exception:
                logger.exception(
                    "Failed to fetch %s for year %d", rt, year
                )
                continue

            total_fetched += len(records)
            logger.info(
                "Found %d %s records for year %d", len(records), rt, year
            )

            if not records:
                continue

            # Deduplicate keeping highest version
            deduped = _deduplicate(records)
            if len(deduped) < len(records):
                logger.info(
                    "Deduplicated %d -> %d records (by key, highest version)",
                    len(records),
                    len(deduped),
                )

            upserted = upsert_financial_statements(
                settings.database_url,
                deduped,
                company_map,
                batch_size=settings.pipeline_batch_size,
            )
            total_upserted += upserted

    logger.info(
        "Financial statements collector finished: fetched=%d upserted=%d",
        total_fetched,
        total_upserted,
    )


def main() -> None:
    """Entry point for python -m pipeline.src.financial_statements_collector."""
    args = parse_args()
    run(args.years, args.report_type)


if __name__ == "__main__":
    main()
