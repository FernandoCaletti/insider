"""Incremental pipeline cron job.

Downloads the CVM document CSV for the current year, identifies new documents
by file_hash, downloads and processes their PDFs, and loads the data into the
database.  A sync_log record tracks each run.

Usage:
    python -m pipeline.src.main
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone
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
from pipeline.src.extractor.pdf_parser import extract_pdf
from pipeline.src.loader.supabase_loader import (
    create_sync_log,
    file_hash_exists,
    get_company_id,
    update_sync_log,
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
# Document processing
# ---------------------------------------------------------------------------

# Delay between PDF downloads (seconds).
DOWNLOAD_DELAY = 2


def _process_document(
    doc: DocumentRecord,
    database_url: str,
    session: Any,
) -> bool:
    """Download, extract, and load a single document.

    Returns True on success, False on failure.
    """
    logger = logging.getLogger(__name__)

    # Look up company
    company_id = get_company_id(database_url, doc.cvm_code)
    if company_id is None:
        logger.warning(
            "Company not found for cvm_code=%s, skipping document", doc.cvm_code
        )
        return False

    # Download PDF
    tmp_path: str | None = None
    try:
        tmp_path = download_pdf(session, doc.document_url)
    except Exception:
        logger.exception("Failed to download PDF: %s", doc.document_url)
        return False

    try:
        # Hash for duplicate control
        file_hash = sha256_hash(tmp_path)
        if file_hash_exists(database_url, file_hash):
            logger.info("Duplicate hash %s, skipping", file_hash[:12])
            return True  # Not a failure

        # Extract
        result = extract_pdf(tmp_path)
        if result.is_scanned:
            logger.warning("Scanned PDF skipped: %s", doc.document_url)
            return False

        # Parse reference date components
        ref_date = doc.reference_date  # YYYY-MM-DD
        try:
            year = int(ref_date[:4])
            month = int(ref_date[5:7])
        except (ValueError, IndexError):
            year = datetime.now(tz=timezone.utc).year
            month = datetime.now(tz=timezone.utc).month

        # Upsert document
        doc_id = upsert_document(
            database_url=database_url,
            company_id=company_id,
            reference_date=ref_date,
            year=year,
            month=month,
            file_name=doc.document_url.split("/")[-1] if doc.document_url else None,
            file_hash=file_hash,
            original_url=doc.document_url,
            page_count=result.page_count,
            is_scanned=result.is_scanned,
        )
        if doc_id is None:
            return False

        # Upsert holdings
        all_holdings = result.all_holdings
        if all_holdings:
            upsert_holdings(database_url, doc_id, all_holdings, batch_size=100)

        logger.info(
            "Processed document cvm=%s ref=%s holdings=%d",
            doc.cvm_code,
            ref_date,
            len(all_holdings),
        )
        return True

    except Exception:
        logger.exception("Error processing document: %s", doc.document_url)
        return False
    finally:
        if tmp_path:
            cleanup_file(tmp_path)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def run_pipeline() -> None:
    """Run the incremental pipeline for the current year."""
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = logging.getLogger(__name__)

    current_year = datetime.now(tz=timezone.utc).year
    logger.info("Starting incremental pipeline for year %d", current_year)

    # Create sync_log
    sync_id = create_sync_log(settings.database_url)

    errors: list[str] = []
    documents_found = 0
    documents_processed = 0
    documents_failed = 0

    try:
        # Fetch document metadata
        documents = fetch_and_parse_documents(current_year, settings.cvm_base_url)
        documents_found = len(documents)
        logger.info("Found %d insider trading documents", documents_found)

        # Filter to only new documents (not yet hashed)
        new_docs: list[DocumentRecord] = []
        for doc in documents:
            # We can't hash without downloading, so we download and check later.
            # But we can skip docs we've seen by checking a lighter signal.
            new_docs.append(doc)

        if not new_docs:
            logger.info("No new documents to process")
            update_sync_log(
                settings.database_url,
                sync_id,
                status="success",
                documents_found=documents_found,
                documents_processed=0,
                documents_failed=0,
            )
            return

        # Create HTTP session for PDF downloads
        session = create_session()
        warmup_session(session)

        for i, doc in enumerate(new_docs):
            if i > 0:
                time.sleep(DOWNLOAD_DELAY)

            try:
                success = _process_document(doc, settings.database_url, session)
                if success:
                    documents_processed += 1
                else:
                    documents_failed += 1
                    errors.append(f"Failed: cvm={doc.cvm_code} ref={doc.reference_date}")
            except Exception:
                documents_failed += 1
                errors.append(f"Error: cvm={doc.cvm_code} ref={doc.reference_date}")
                logger.exception(
                    "Unhandled error for cvm=%s ref=%s",
                    doc.cvm_code,
                    doc.reference_date,
                )

        # Determine final status
        total_attempted = documents_processed + documents_failed
        if total_attempted > 0 and documents_failed / total_attempted > 0.5:
            status = "error"
        else:
            status = "success"

        logger.info(
            "Pipeline finished: found=%d processed=%d failed=%d status=%s",
            documents_found,
            documents_processed,
            documents_failed,
            status,
        )

    except Exception as exc:
        status = "error"
        errors.append(str(exc))
        logger.exception("Pipeline failed with critical error")

    update_sync_log(
        settings.database_url,
        sync_id,
        status=status,
        documents_found=documents_found,
        documents_processed=documents_processed,
        documents_failed=documents_failed,
        error_details={"errors": errors} if errors else None,
    )


def main() -> None:
    """Entry point for python -m pipeline.src.main."""
    run_pipeline()


if __name__ == "__main__":
    main()
