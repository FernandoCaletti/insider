"""Material facts collector — fetch Fatos Relevantes from CVM.

Downloads the CVM IPE CSV for each requested year, filters for
"Fato Relevante" entries, and upserts them into the material_facts table.
Idempotent via the protocol UNIQUE constraint.

Usage:
    python -m pipeline.src.material_facts_collector
    python -m pipeline.src.material_facts_collector --years 2023 2024 2025
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
    fetch_and_parse_material_facts,
)
from pipeline.src.config import get_settings
from pipeline.src.loader.supabase_loader import upsert_material_facts


# ---------------------------------------------------------------------------
# Structured JSON logging (reused pattern from main.py / backfill.py)
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
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Collect material facts (Fatos Relevantes) from CVM.",
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
    """Fetch and load material facts for the given years."""
    settings = get_settings()
    setup_logging(settings.log_level)

    logger.info("Material facts collector starting for years: %s", years)

    # Pre-load company map to avoid N+1 lookups
    company_map = _load_company_map(settings.database_url)
    logger.info("Loaded %d companies from database", len(company_map))

    total_fetched = 0
    total_upserted = 0

    for year in years:
        logger.info("Fetching material facts for year %d", year)
        try:
            records = fetch_and_parse_material_facts(
                year, settings.cvm_base_url
            )
        except Exception:
            logger.exception("Failed to fetch material facts for year %d", year)
            continue

        total_fetched += len(records)
        logger.info(
            "Found %d material fact records for year %d", len(records), year
        )

        if not records:
            continue

        # Deduplicate by protocol, keeping the highest version
        by_protocol: dict[str, Any] = {}
        for r in records:
            existing = by_protocol.get(r.protocol)
            if existing is None or r.version > existing.version:
                by_protocol[r.protocol] = r
        deduped = list(by_protocol.values())

        if len(deduped) < len(records):
            logger.info(
                "Deduplicated %d -> %d records (by protocol, highest version)",
                len(records),
                len(deduped),
            )

        upserted = upsert_material_facts(
            settings.database_url,
            deduped,
            company_map,
            batch_size=settings.pipeline_batch_size,
        )
        total_upserted += upserted

    logger.info(
        "Material facts collector finished: fetched=%d upserted=%d",
        total_fetched,
        total_upserted,
    )


def main() -> None:
    """Entry point for python -m pipeline.src.material_facts_collector."""
    args = parse_args()
    run(args.years)


if __name__ == "__main__":
    main()
