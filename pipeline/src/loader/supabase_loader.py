"""Database loader for upserting data into Supabase/PostgreSQL."""

import json
import logging
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

from pipeline.src.collector.cvm_client import CompanyRecord, MaterialFactRecord
from pipeline.src.extractor.pdf_parser import HoldingRecord

logger = logging.getLogger(__name__)

TICKER_MAPPING_PATH = (
    Path(__file__).resolve().parent.parent.parent / "data" / "ticker_mapping.json"
)


def _load_ticker_mapping(path: Path = TICKER_MAPPING_PATH) -> dict[str, str]:
    """Load CVM code to ticker symbol mapping from JSON file."""
    with open(path, encoding="utf-8") as f:
        mapping: dict[str, str] = json.load(f)
    logger.info("Loaded ticker mapping with %d entries", len(mapping))
    return mapping


def _decimal_to_float(val: Decimal | None) -> float | None:
    """Convert Decimal to float for psycopg2 compatibility."""
    if val is None:
        return None
    return float(val)


def upsert_companies(
    database_url: str,
    companies: list[CompanyRecord],
    batch_size: int = 100,
) -> int:
    """Upsert company records into the companies table.

    Uses PostgreSQL ON CONFLICT to insert or update companies by cvm_code.
    Ticker is populated from ticker_mapping.json during upsert.
    Companies without a ticker mapping get ticker = NULL.

    Args:
        database_url: PostgreSQL connection string.
        companies: List of company records to upsert.
        batch_size: Number of records per batch.

    Returns:
        Number of records upserted.
    """
    ticker_mapping = _load_ticker_mapping()

    upsert_sql = """
        INSERT INTO companies (cvm_code, name, cnpj, ticker, sector, subsector, is_active)
        VALUES (%(cvm_code)s, %(name)s, %(cnpj)s, %(ticker)s, %(sector)s, %(subsector)s, %(is_active)s)
        ON CONFLICT (cvm_code) DO UPDATE SET
            name = EXCLUDED.name,
            cnpj = EXCLUDED.cnpj,
            ticker = EXCLUDED.ticker,
            sector = EXCLUDED.sector,
            subsector = EXCLUDED.subsector,
            is_active = EXCLUDED.is_active,
            updated_at = NOW()
    """

    total_upserted = 0

    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            for i in range(0, len(companies), batch_size):
                batch = companies[i : i + batch_size]
                params = [
                    {
                        "cvm_code": c.cvm_code,
                        "name": c.name,
                        "cnpj": c.cnpj,
                        "ticker": ticker_mapping.get(c.cvm_code),
                        "sector": c.sector,
                        "subsector": c.subsector,
                        "is_active": c.is_active,
                    }
                    for c in batch
                ]
                psycopg2.extras.execute_batch(cur, upsert_sql, params)
                total_upserted += len(batch)
                logger.info(
                    "Upserted batch %d-%d (%d records)",
                    i,
                    i + len(batch),
                    len(batch),
                )

        conn.commit()
        logger.info("Total companies upserted: %d", total_upserted)
    except Exception:
        conn.rollback()
        logger.exception("Error upserting companies, transaction rolled back")
        raise
    finally:
        conn.close()

    return total_upserted


# ---------------------------------------------------------------------------
# Sync log operations
# ---------------------------------------------------------------------------


def create_sync_log(database_url: str) -> int:
    """Create a new sync_log record with status 'running'.

    Returns:
        The id of the created sync_log record.
    """
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sync_log (status) VALUES ('running') RETURNING id"
            )
            row = cur.fetchone()
            assert row is not None
            sync_id: int = row[0]
        conn.commit()
        logger.info("Created sync_log record id=%d", sync_id)
        return sync_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_sync_log(
    database_url: str,
    sync_id: int,
    status: str,
    documents_found: int = 0,
    documents_processed: int = 0,
    documents_failed: int = 0,
    error_details: dict[str, Any] | None = None,
) -> None:
    """Update a sync_log record with final status and counts."""
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sync_log SET
                    status = %(status)s,
                    finished_at = NOW(),
                    documents_found = %(documents_found)s,
                    documents_processed = %(documents_processed)s,
                    documents_failed = %(documents_failed)s,
                    error_details = %(error_details)s
                WHERE id = %(sync_id)s
                """,
                {
                    "sync_id": sync_id,
                    "status": status,
                    "documents_found": documents_found,
                    "documents_processed": documents_processed,
                    "documents_failed": documents_failed,
                    "error_details": json.dumps(error_details) if error_details else None,
                },
            )
        conn.commit()
        logger.info("Updated sync_log id=%d status=%s", sync_id, status)
    except Exception:
        conn.rollback()
        logger.exception("Failed to update sync_log id=%d", sync_id)
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# File hash checking
# ---------------------------------------------------------------------------


def file_hash_exists(database_url: str, file_hash: str) -> bool:
    """Check if a document with the given file_hash already exists."""
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM documents WHERE file_hash = %s LIMIT 1",
                (file_hash,),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def get_company_id(database_url: str, cvm_code: str) -> int | None:
    """Look up a company's id by cvm_code."""
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM companies WHERE cvm_code = %s",
                (cvm_code,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Document and holdings upsert
# ---------------------------------------------------------------------------


def upsert_document(
    database_url: str,
    company_id: int,
    reference_date: str,
    year: int,
    month: int,
    file_name: str | None,
    file_hash: str,
    original_url: str | None,
    page_count: int,
    is_scanned: bool,
) -> int | None:
    """Insert or update a document record.

    Uses ON CONFLICT on file_hash. Returns the document id, or None on error.
    """
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO documents
                    (company_id, reference_date, year, month, file_name,
                     file_hash, original_url, page_count, is_scanned)
                VALUES
                    (%(company_id)s, %(reference_date)s, %(year)s, %(month)s,
                     %(file_name)s, %(file_hash)s, %(original_url)s,
                     %(page_count)s, %(is_scanned)s)
                ON CONFLICT (file_hash) DO UPDATE SET
                    page_count = EXCLUDED.page_count,
                    is_scanned = EXCLUDED.is_scanned,
                    processed_at = NOW()
                RETURNING id
                """,
                {
                    "company_id": company_id,
                    "reference_date": reference_date,
                    "year": year,
                    "month": month,
                    "file_name": file_name,
                    "file_hash": file_hash,
                    "original_url": original_url,
                    "page_count": page_count,
                    "is_scanned": is_scanned,
                },
            )
            row = cur.fetchone()
            assert row is not None
            doc_id: int = row[0]
        conn.commit()
        logger.info("Upserted document id=%d hash=%s", doc_id, file_hash[:12])
        return doc_id
    except Exception:
        conn.rollback()
        logger.exception("Error upserting document hash=%s", file_hash[:12])
        return None
    finally:
        conn.close()


def upsert_holdings(
    database_url: str,
    document_id: int,
    holdings: list[HoldingRecord],
    batch_size: int = 100,
) -> int:
    """Batch upsert holdings for a document.

    Deletes existing holdings for the document first (replace strategy),
    then inserts new ones in batches.

    Args:
        database_url: PostgreSQL connection string.
        document_id: The document id to associate holdings with.
        holdings: List of HoldingRecord from PDF extraction.
        batch_size: Number of records per batch.

    Returns:
        Number of holdings inserted.
    """
    if not holdings:
        return 0

    insert_sql = """
        INSERT INTO holdings
            (document_id, section, asset_type, asset_description, quantity,
             unit_price, total_value, operation_type, operation_date, broker,
             confidence, insider_group, insider_name, transaction_day)
        VALUES
            (%(document_id)s, %(section)s, %(asset_type)s, %(asset_description)s,
             %(quantity)s, %(unit_price)s, %(total_value)s, %(operation_type)s,
             %(operation_date)s, %(broker)s, %(confidence)s, %(insider_group)s,
             %(insider_name)s, %(transaction_day)s)
    """

    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            # Remove old holdings for this document before re-inserting
            cur.execute("DELETE FROM holdings WHERE document_id = %s", (document_id,))

            total_inserted = 0
            for i in range(0, len(holdings), batch_size):
                batch = holdings[i : i + batch_size]
                params = [
                    {
                        "document_id": document_id,
                        "section": h.section,
                        "asset_type": h.asset_type,
                        "asset_description": h.asset_description,
                        "quantity": _decimal_to_float(h.quantity),
                        "unit_price": _decimal_to_float(h.unit_price),
                        "total_value": _decimal_to_float(h.total_value),
                        "operation_type": h.operation_type,
                        "operation_date": h.operation_date,
                        "broker": h.broker,
                        "confidence": h.confidence,
                        "insider_group": h.insider_group,
                        "insider_name": h.insider_name,
                        "transaction_day": h.transaction_day,
                    }
                    for h in batch
                ]
                psycopg2.extras.execute_batch(cur, insert_sql, params)
                total_inserted += len(batch)

        conn.commit()
        logger.info(
            "Inserted %d holdings for document_id=%d", total_inserted, document_id
        )
        return total_inserted
    except Exception:
        conn.rollback()
        logger.exception("Error inserting holdings for document_id=%d", document_id)
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Material facts upsert
# ---------------------------------------------------------------------------


def upsert_material_facts(
    database_url: str,
    facts: list[MaterialFactRecord],
    company_map: dict[str, int],
    batch_size: int = 100,
) -> int:
    """Batch upsert material fact records into the material_facts table.

    Uses ON CONFLICT (protocol) for idempotent imports. Skips records
    whose cvm_code is not found in company_map.

    Args:
        database_url: PostgreSQL connection string.
        facts: List of MaterialFactRecord from CVM CSV.
        company_map: Mapping of cvm_code -> company_id.
        batch_size: Number of records per batch.

    Returns:
        Number of records upserted.
    """
    if not facts:
        return 0

    upsert_sql = """
        INSERT INTO material_facts
            (company_id, reference_date, category, subject, source_url,
             cvm_code, protocol, delivery_date)
        VALUES
            (%(company_id)s, %(reference_date)s, %(category)s, %(subject)s,
             %(source_url)s, %(cvm_code)s, %(protocol)s, %(delivery_date)s)
        ON CONFLICT (protocol) DO UPDATE SET
            category = EXCLUDED.category,
            subject = EXCLUDED.subject,
            source_url = EXCLUDED.source_url,
            delivery_date = EXCLUDED.delivery_date
    """

    conn = psycopg2.connect(database_url)
    try:
        total_upserted = 0
        skipped = 0
        with conn.cursor() as cur:
            for i in range(0, len(facts), batch_size):
                batch = facts[i : i + batch_size]
                params: list[dict[str, Any]] = []
                for f in batch:
                    company_id = company_map.get(f.cvm_code)
                    if company_id is None:
                        skipped += 1
                        continue
                    params.append(
                        {
                            "company_id": company_id,
                            "reference_date": f.reference_date or None,
                            "category": f.category or None,
                            "subject": f.subject or None,
                            "source_url": f.source_url or None,
                            "cvm_code": f.cvm_code,
                            "protocol": f.protocol,
                            "delivery_date": f.delivery_date or None,
                        }
                    )
                if params:
                    psycopg2.extras.execute_batch(cur, upsert_sql, params)
                    total_upserted += len(params)

        conn.commit()
        logger.info(
            "Upserted %d material facts (%d skipped - unknown company)",
            total_upserted,
            skipped,
        )
        return total_upserted
    except Exception:
        conn.rollback()
        logger.exception("Error upserting material facts")
        raise
    finally:
        conn.close()
