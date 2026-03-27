"""Document endpoints."""

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from api.app.database import get_cursor

router = APIRouter(prefix="/documents", tags=["documents"])

# Alias DB columns to match frontend Document type
_DOCUMENT_COLUMNS = """
    d.id,
    d.company_id,
    d.reference_date,
    d.file_hash,
    d.file_name AS filename,
    d.original_url AS source_url,
    d.year,
    d.month,
    d.page_count,
    d.is_scanned,
    d.processed_at,
    d.created_at
"""


@router.get("")
async def list_documents(
    company_id: int | None = None,
    year: int | None = None,
    month: int | None = Query(None, ge=1, le=12),
    search: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """List documents with optional filters."""
    offset = (page - 1) * per_page

    with get_cursor() as cur:
        conditions: list[str] = []
        params: list[Any] = []

        if company_id is not None:
            conditions.append("d.company_id = %s")
            params.append(company_id)

        if year is not None:
            conditions.append("d.year = %s")
            params.append(year)

        if month is not None:
            conditions.append("d.month = %s")
            params.append(month)

        if search:
            conditions.append("(c.name ILIKE %s OR c.ticker ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count
        count_params = list(params)
        cur.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM documents d
            JOIN companies c ON c.id = d.company_id
            {where}
            """,
            count_params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Paginated results with company info
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT {_DOCUMENT_COLUMNS},
                   c.name AS company_name,
                   c.ticker AS company_ticker
            FROM documents d
            JOIN companies c ON c.id = d.company_id
            {where}
            ORDER BY d.reference_date DESC, d.id DESC
            LIMIT %s OFFSET %s
            """,
            params,
        )
        rows = cur.fetchall()

    return {
        "data": [dict(r) for r in rows],  # type: ignore[arg-type]
        "total": total,
        "page": page,
        "per_page": per_page,
    }


@router.get("/{document_id}")
async def get_document(document_id: int) -> dict[str, Any]:
    """Get a single document with company info and holding counts."""
    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT {_DOCUMENT_COLUMNS},
                   c.name AS company_name,
                   c.ticker AS company_ticker,
                   c.cvm_code
            FROM documents d
            JOIN companies c ON c.id = d.company_id
            WHERE d.id = %s
            """,
            (document_id,),
        )
        doc = cur.fetchone()
        if not doc:
            raise HTTPException(
                status_code=404,
                detail={"code": "NOT_FOUND", "message": "Documento nao encontrado"},
            )

        # Get holding counts by section
        cur.execute(
            """
            SELECT section, COUNT(*) AS cnt
            FROM holdings
            WHERE document_id = %s
            GROUP BY section
            """,
            (document_id,),
        )
        section_counts = {row["section"]: row["cnt"] for row in cur.fetchall()}  # type: ignore[index]

    return {
        "data": {
            **dict(doc),  # type: ignore[arg-type]
            "holdings_count": section_counts,
        }
    }
