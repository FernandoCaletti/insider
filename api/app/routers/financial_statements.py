"""Financial statements endpoints."""

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from api.app.database import get_cursor

router = APIRouter(prefix="/financial-statements", tags=["financial-statements"])

_VALID_STATEMENT_TYPES = {"BPA", "BPP", "DRE", "DFC_MI"}


@router.get("/summary")
async def financial_summary(
    company_id: int | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    """Get aggregate summary of financial statements."""
    with get_cursor() as cur:
        conditions: list[str] = []
        params: list[Any] = []

        if company_id is not None:
            conditions.append("fs.company_id = %s")
            params.append(company_id)

        if date_from is not None:
            conditions.append("fs.reference_date >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("fs.reference_date <= %s")
            params.append(date_to)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        cur.execute(
            f"""
            SELECT
                COUNT(*) AS total_records,
                COUNT(DISTINCT fs.company_id) AS companies_count,
                COUNT(DISTINCT fs.statement_type) AS statement_types_count,
                MIN(fs.reference_date) AS earliest_date,
                MAX(fs.reference_date) AS latest_date
            FROM financial_statements fs
            {where}
            """,
            params,
        )
        summary = cur.fetchone()

        # Breakdown by statement type
        cur.execute(
            f"""
            SELECT fs.statement_type, COUNT(*) AS count
            FROM financial_statements fs
            {where}
            GROUP BY fs.statement_type
            ORDER BY fs.statement_type
            """,
            params,
        )
        by_type = cur.fetchall()

    return {
        "data": {
            **dict(summary),  # type: ignore[arg-type]
            "by_type": [dict(r) for r in by_type],  # type: ignore[arg-type]
        }
    }


@router.get("")
async def list_financial_statements(
    company_id: int | None = None,
    statement_type: str | None = None,
    account_code: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    search: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """List financial statements with optional filters."""
    if statement_type and statement_type.upper() not in _VALID_STATEMENT_TYPES:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_STATEMENT_TYPE",
                "message": f"Tipo invalido. Valores aceitos: {', '.join(sorted(_VALID_STATEMENT_TYPES))}",
            },
        )

    offset = (page - 1) * per_page

    with get_cursor() as cur:
        conditions: list[str] = []
        params: list[Any] = []

        if company_id is not None:
            conditions.append("fs.company_id = %s")
            params.append(company_id)

        if statement_type:
            conditions.append("fs.statement_type = %s")
            params.append(statement_type.upper())

        if account_code:
            conditions.append("fs.account_code = %s")
            params.append(account_code)

        if date_from is not None:
            conditions.append("fs.reference_date >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("fs.reference_date <= %s")
            params.append(date_to)

        if search:
            conditions.append(
                "(fs.account_name ILIKE %s OR c.name ILIKE %s OR c.ticker ILIKE %s)"
            )
            params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count
        count_params = list(params)
        cur.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM financial_statements fs
            JOIN companies c ON c.id = fs.company_id
            {where}
            """,
            count_params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Paginated results with company info
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT fs.id,
                   fs.company_id,
                   fs.reference_date,
                   fs.statement_type,
                   fs.account_code,
                   fs.account_name,
                   fs.value,
                   fs.currency,
                   fs.source_url,
                   fs.created_at,
                   c.name AS company_name,
                   c.ticker AS company_ticker
            FROM financial_statements fs
            JOIN companies c ON c.id = fs.company_id
            {where}
            ORDER BY fs.reference_date DESC, fs.statement_type, fs.account_code
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


@router.get("/{statement_id}")
async def get_financial_statement(statement_id: int) -> dict[str, Any]:
    """Get a single financial statement with company info."""
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT fs.*,
                   c.name AS company_name,
                   c.ticker AS company_ticker,
                   c.cvm_code
            FROM financial_statements fs
            JOIN companies c ON c.id = fs.company_id
            WHERE fs.id = %s
            """,
            (statement_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "NOT_FOUND",
                    "message": "Demonstracao financeira nao encontrada",
                },
            )

    return {"data": dict(row)}  # type: ignore[arg-type]
