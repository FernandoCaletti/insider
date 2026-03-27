"""Correlation analysis between insider movements and material facts."""

from datetime import date
from typing import Any

from fastapi import APIRouter, Query

from api.app.database import get_cursor

router = APIRouter(prefix="/correlations", tags=["correlations"])


@router.get("")
async def list_correlations(
    company_id: int | None = None,
    days_window: int = Query(30, ge=1, le=365),
    date_from: date | None = None,
    date_to: date | None = None,
    operation_type: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
) -> dict[str, Any]:
    """Find insider movements near material fact dates for the same company.

    Returns pairs of (movement, material_fact) where the movement's
    operation_date falls within ``days_window`` of the fact's
    reference_date.
    """
    offset = (page - 1) * per_page

    with get_cursor() as cur:
        conditions: list[str] = [
            "h.section = 'movimentacoes'",
            "h.confidence != 'baixa'",
            "ABS(h.operation_date - mf.reference_date) <= %s",
        ]
        params: list[Any] = [days_window]

        if company_id is not None:
            conditions.append("d.company_id = %s")
            params.append(company_id)

        if date_from is not None:
            conditions.append("h.operation_date >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("h.operation_date <= %s")
            params.append(date_to)

        if operation_type is not None:
            conditions.append("h.operation_type ILIKE %s")
            params.append(operation_type)

        where = "WHERE " + " AND ".join(conditions)

        # Count
        count_params = list(params)
        cur.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            JOIN material_facts mf
                ON mf.company_id = d.company_id
            {where}
            """,
            count_params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Paginated results
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT
                h.id              AS holding_id,
                h.operation_date,
                h.operation_type,
                h.asset_type,
                h.total_value,
                h.quantity,
                h.insider_group,
                mf.id             AS material_fact_id,
                mf.reference_date AS fact_date,
                mf.category       AS fact_category,
                mf.subject        AS fact_subject,
                (h.operation_date - mf.reference_date) AS days_diff,
                c.id              AS company_id,
                c.name            AS company_name,
                c.ticker          AS company_ticker
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            JOIN companies c ON c.id = d.company_id
            JOIN material_facts mf
                ON mf.company_id = d.company_id
            {where}
            ORDER BY ABS(h.operation_date - mf.reference_date),
                     h.operation_date DESC,
                     h.id DESC
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


@router.get("/summary")
async def correlation_summary(
    days_window: int = Query(30, ge=1, le=365),
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    """Summary statistics for correlation analysis."""
    with get_cursor() as cur:
        conditions: list[str] = [
            "h.section = 'movimentacoes'",
            "h.confidence != 'baixa'",
            "ABS(h.operation_date - mf.reference_date) <= %s",
        ]
        params: list[Any] = [days_window]

        if date_from is not None:
            conditions.append("h.operation_date >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("h.operation_date <= %s")
            params.append(date_to)

        where = "WHERE " + " AND ".join(conditions)

        cur.execute(
            f"""
            SELECT
                COUNT(*)                                        AS total_correlations,
                COUNT(DISTINCT d.company_id)                    AS companies_involved,
                COUNT(DISTINCT h.id)                            AS unique_movements,
                COUNT(DISTINCT mf.id)                           AS unique_facts,
                COALESCE(SUM(h.total_value), 0)                 AS total_value,
                ROUND(AVG(ABS(h.operation_date - mf.reference_date)), 1)
                                                                AS avg_days_diff,
                COUNT(*) FILTER (WHERE h.operation_date < mf.reference_date)
                                                                AS movements_before_fact,
                COUNT(*) FILTER (WHERE h.operation_date >= mf.reference_date)
                                                                AS movements_after_fact
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            JOIN material_facts mf
                ON mf.company_id = d.company_id
            {where}
            """,
            params,
        )
        row = cur.fetchone()

    return {"data": dict(row)}  # type: ignore[arg-type]


@router.get("/top-companies")
async def top_correlated_companies(
    days_window: int = Query(30, ge=1, le=365),
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int = Query(10, ge=1, le=50),
) -> dict[str, Any]:
    """Companies with the most correlated movements near material facts."""
    with get_cursor() as cur:
        conditions: list[str] = [
            "h.section = 'movimentacoes'",
            "h.confidence != 'baixa'",
            "ABS(h.operation_date - mf.reference_date) <= %s",
        ]
        params: list[Any] = [days_window]

        if date_from is not None:
            conditions.append("h.operation_date >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("h.operation_date <= %s")
            params.append(date_to)

        where = "WHERE " + " AND ".join(conditions)

        params.append(limit)
        cur.execute(
            f"""
            SELECT
                c.id                                 AS company_id,
                c.name                               AS company_name,
                c.ticker                             AS company_ticker,
                COUNT(*)                             AS correlation_count,
                COUNT(DISTINCT h.id)                 AS unique_movements,
                COUNT(DISTINCT mf.id)                AS unique_facts,
                COALESCE(SUM(h.total_value), 0)      AS total_value,
                ROUND(AVG(ABS(h.operation_date - mf.reference_date)), 1)
                                                     AS avg_days_diff
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            JOIN companies c ON c.id = d.company_id
            JOIN material_facts mf
                ON mf.company_id = d.company_id
            {where}
            GROUP BY c.id, c.name, c.ticker
            ORDER BY correlation_count DESC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()

    return {"data": [dict(r) for r in rows]}  # type: ignore[arg-type]
