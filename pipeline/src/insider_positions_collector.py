"""Insider positions collector — fetch individual position data from CVM FRE.

Downloads the CVM FRE (Formulário de Referência) ZIP for each requested year,
extracts the position CSV, parses insider position records, and upserts them
into the insider_positions table.
Idempotent via the (company_id, insider_name, reference_date, asset_type)
UNIQUE constraint.

Usage:
    python -m pipeline.src.insider_positions_collector
    python -m pipeline.src.insider_positions_collector --years 2023 2024 2025
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
    InsiderPositionRecord,
    fetch_and_parse_positions,
)
from pipeline.src.config import get_settings
from pipeline.src.loader.supabase_loader import upsert_insider_positions


# ---------------------------------------------------------------------------
# Structured JSON logging (reused pattern from other collectors)
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
    records: list[InsiderPositionRecord],
) -> list[InsiderPositionRecord]:
    """Deduplicate by (cvm_code, insider_name, reference_date, asset_type).

    Keeps the record with the highest version.
    """
    by_key: dict[tuple[str, str, str, str], InsiderPositionRecord] = {}
    for r in records:
        key = (r.cvm_code, r.insider_name, r.reference_date, r.asset_type)
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
        description="Collect individual insider position data from CVM FRE filings.",
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
    """Fetch and load insider positions for the given years."""
    settings = get_settings()
    setup_logging(settings.log_level)

    logger.info("Insider positions collector starting: years=%s", years)

    # Pre-load company map
    company_map = _load_company_map(settings.database_url)
    logger.info("Loaded %d companies from database", len(company_map))

    total_fetched = 0
    total_upserted = 0

    for year in years:
        logger.info("Fetching insider positions for year %d", year)
        try:
            records = fetch_and_parse_positions(year, settings.cvm_base_url)
        except Exception:
            logger.exception(
                "Failed to fetch insider positions for year %d", year
            )
            continue

        total_fetched += len(records)
        logger.info(
            "Found %d insider position records for year %d", len(records), year
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

        upserted = upsert_insider_positions(
            settings.database_url,
            deduped,
            company_map,
            batch_size=settings.pipeline_batch_size,
        )
        total_upserted += upserted

    logger.info(
        "Insider positions collector finished: fetched=%d upserted=%d",
        total_fetched,
        total_upserted,
    )


def main() -> None:
    """Entry point for python -m pipeline.src.insider_positions_collector."""
    args = parse_args()
    run(args.years)


if __name__ == "__main__":
    main()
