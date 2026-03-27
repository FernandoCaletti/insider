"""Material facts endpoints."""

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from api.app.database import get_cursor

router = APIRouter(prefix="/material-facts", tags=["material-facts"])


@router.get("")
async def list_material_facts(
    company_id: int | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    category: str | None = None,
    search: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """List material facts with optional filters."""
    offset = (page - 1) * per_page

    with get_cursor() as cur:
        conditions: list[str] = []
        params: list[Any] = []

        if company_id is not None:
            conditions.append("mf.company_id = %s")
            params.append(company_id)

        if date_from is not None:
            conditions.append("mf.reference_date >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("mf.reference_date <= %s")
            params.append(date_to)

        if category:
            conditions.append("mf.category ILIKE %s")
            params.append(f"%{category}%")

        if search:
            conditions.append(
                "(mf.subject ILIKE %s OR c.name ILIKE %s OR c.ticker ILIKE %s)"
            )
            params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count
        count_params = list(params)
        cur.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM material_facts mf
            JOIN companies c ON c.id = mf.company_id
            {where}
            """,
            count_params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Paginated results with company info
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT mf.id,
                   mf.company_id,
                   mf.reference_date,
                   mf.category,
                   mf.subject,
                   mf.source_url,
                   mf.cvm_code,
                   mf.protocol,
                   mf.delivery_date,
                   mf.created_at,
                   c.name AS company_name,
                   c.ticker AS company_ticker
            FROM material_facts mf
            JOIN companies c ON c.id = mf.company_id
            {where}
            ORDER BY mf.reference_date DESC, mf.id DESC
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


@router.get("/{material_fact_id}")
async def get_material_fact(material_fact_id: int) -> dict[str, Any]:
    """Get a single material fact with company info."""
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT mf.*,
                   c.name AS company_name,
                   c.ticker AS company_ticker,
                   c.cvm_code
            FROM material_facts mf
            JOIN companies c ON c.id = mf.company_id
            WHERE mf.id = %s
            """,
            (material_fact_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "NOT_FOUND",
                    "message": "Fato relevante nao encontrado",
                },
            )

    return {"data": dict(row)}  # type: ignore[arg-type]
