"""Dividends collector — fetch proventos data from CVM FCA filings.

Downloads the CVM FCA (Formulário Cadastral) ZIP for each requested year,
extracts the proventos em dinheiro CSV, parses dividend records, and upserts
them into the dividends table.
Idempotent via the (company_id, ex_date, dividend_type) UNIQUE constraint.

Usage:
    python -m pipeline.src.dividends_collector
    python -m pipeline.src.dividends_collector --years 2023 2024 2025
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
    DividendRecord,
    fetch_and_parse_dividends,
)
from pipeline.src.config import get_settings
from pipeline.src.loader.supabase_loader import upsert_dividends


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
    records: list[DividendRecord],
) -> list[DividendRecord]:
    """Deduplicate by (cvm_code, ex_date, dividend_type).

    Keeps the record with the highest version.
    """
    by_key: dict[tuple[str, str, str], DividendRecord] = {}
    for r in records:
        key = (r.cvm_code, r.ex_date, r.dividend_type)
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
        description="Collect dividend (proventos) data from CVM FCA filings.",
    )
    current_year = datetime.now(tz=timezone.utc).year
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=[current_year],
        help="Years to fetch (default: current year).",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(years: list[int]) -> None:
    """Fetch and load dividends for the given years."""
    settings = get_settings()
    setup_logging(settings.log_level)

    logger.info("Dividends collector starting: years=%s", years)

    # Pre-load company map
    company_map = _load_company_map(settings.database_url)
    logger.info("Loaded %d companies from database", len(company_map))

    total_fetched = 0
    total_upserted = 0

    for year in years:
        logger.info("Fetching dividends for year %d", year)
        try:
            records = fetch_and_parse_dividends(year, settings.cvm_base_url)
        except Exception:
            logger.exception("Failed to fetch dividends for year %d", year)
            continue

        total_fetched += len(records)
        logger.info("Found %d dividend records for year %d", len(records), year)

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

        upserted = upsert_dividends(
            settings.database_url,
            deduped,
            company_map,
            batch_size=settings.pipeline_batch_size,
        )
        total_upserted += upserted

    logger.info(
        "Dividends collector finished: fetched=%d upserted=%d",
        total_fetched,
        total_upserted,
    )


def main() -> None:
    """Entry point for python -m pipeline.src.dividends_collector."""
    args = parse_args()
    run(args.years)


if __name__ == "__main__":
    main()
