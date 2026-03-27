"""Company enrichment script.

Downloads cadastral data from CVM and upserts into the companies table,
enriching with ticker symbols from ticker_mapping.json.

Usage:
    python -m pipeline.src.enrich_companies
"""

import logging
import sys

from pipeline.src.collector.cvm_client import fetch_and_parse_companies
from pipeline.src.config import get_settings
from pipeline.src.loader.supabase_loader import upsert_companies


def setup_logging(level: str = "INFO") -> None:
    """Configure structured logging to stdout."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )


def main() -> None:
    """Run company enrichment pipeline."""
    settings = get_settings()
    setup_logging(settings.log_level)

    logger = logging.getLogger(__name__)
    logger.info("Starting company enrichment pipeline")

    companies = fetch_and_parse_companies(settings.cvm_base_url)
    logger.info("Fetched %d companies from CVM", len(companies))

    count = upsert_companies(
        database_url=settings.database_url,
        companies=companies,
        batch_size=settings.pipeline_batch_size,
    )

    logger.info("Company enrichment complete: %d companies upserted", count)


if __name__ == "__main__":
    main()
