"""Dividends endpoints."""

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from api.app.database import get_cursor

router = APIRouter(prefix="/dividends", tags=["dividends"])


@router.get("/summary")
async def dividends_summary(
    company_id: int | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    """Get aggregate summary of dividends."""
    with get_cursor() as cur:
        conditions: list[str] = []
        params: list[Any] = []

        if company_id is not None:
            conditions.append("d.company_id = %s")
            params.append(company_id)

        if date_from is not None:
            conditions.append("d.ex_date >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("d.ex_date <= %s")
            params.append(date_to)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        cur.execute(
            f"""
            SELECT
                COUNT(*) AS total_records,
                COUNT(DISTINCT d.company_id) AS companies_count,
                SUM(d.total_value) AS total_distributed,
                AVG(d.value_per_share) AS avg_value_per_share,
                MIN(d.ex_date) AS earliest_date,
                MAX(d.ex_date) AS latest_date
            FROM dividends d
            {where}
            """,
            params,
        )
        summary = cur.fetchone()

        # Breakdown by dividend type
        cur.execute(
            f"""
            SELECT d.dividend_type, COUNT(*) AS count, SUM(d.total_value) AS total_value
            FROM dividends d
            {where}
            GROUP BY d.dividend_type
            ORDER BY d.dividend_type
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
async def list_dividends(
    company_id: int | None = None,
    dividend_type: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    search: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """List dividends with optional filters."""
    offset = (page - 1) * per_page

    with get_cursor() as cur:
        conditions: list[str] = []
        params: list[Any] = []

        if company_id is not None:
            conditions.append("dv.company_id = %s")
            params.append(company_id)

        if dividend_type:
            conditions.append("dv.dividend_type ILIKE %s")
            params.append(f"%{dividend_type}%")

        if date_from is not None:
            conditions.append("dv.ex_date >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("dv.ex_date <= %s")
            params.append(date_to)

        if search:
            conditions.append("(c.name ILIKE %s OR c.ticker ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count
        count_params = list(params)
        cur.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM dividends dv
            JOIN companies c ON c.id = dv.company_id
            {where}
            """,
            count_params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Paginated results with company info
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT dv.id,
                   dv.company_id,
                   dv.ex_date,
                   dv.payment_date,
                   dv.record_date,
                   dv.dividend_type,
                   dv.value_per_share,
                   dv.total_value,
                   dv.currency,
                   dv.source_url,
                   dv.created_at,
                   c.name AS company_name,
                   c.ticker AS company_ticker
            FROM dividends dv
            JOIN companies c ON c.id = dv.company_id
            {where}
            ORDER BY dv.ex_date DESC, dv.id DESC
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


@router.get("/{dividend_id}")
async def get_dividend(dividend_id: int) -> dict[str, Any]:
    """Get a single dividend with company info."""
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT dv.*,
                   c.name AS company_name,
                   c.ticker AS company_ticker,
                   c.cvm_code
            FROM dividends dv
            JOIN companies c ON c.id = dv.company_id
            WHERE dv.id = %s
            """,
            (dividend_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "NOT_FOUND",
                    "message": "Dividendo nao encontrado",
                },
            )

    return {"data": dict(row)}  # type: ignore[arg-type]
