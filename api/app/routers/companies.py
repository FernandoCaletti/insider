"""Company endpoints."""

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from api.app.database import get_cursor
from api.app.routers.holdings import _validate_insider_group

router = APIRouter(prefix="/companies", tags=["companies"])


def _not_found() -> HTTPException:
    return HTTPException(
        status_code=404,
        detail={"code": "NOT_FOUND", "message": "Empresa nao encontrada"},
    )


@router.get("")
async def list_companies(
    search: str | None = None,
    sector: str | None = None,
    is_active: bool | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """List and search companies with document stats."""
    offset = (page - 1) * per_page

    with get_cursor() as cur:
        # Build conditions
        conditions: list[str] = []
        params: list[Any] = []

        if search:
            # Use search_companies function as source
            source = "search_companies(%s) c"
            params.append(search)
        else:
            source = "companies c"

        if sector:
            conditions.append("c.sector ILIKE %s")
            params.append(f"%{sector}%")

        if is_active is not None:
            conditions.append("c.is_active = %s")
            params.append(is_active)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count total
        cur.execute(
            f"SELECT COUNT(*) AS cnt FROM {source} {where}",
            params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Get paginated results with document stats
        data_params = list(params)
        data_params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT c.*,
                   COALESCE(ds.total_documents, 0) AS total_documents,
                   ds.last_document
            FROM {source}
            LEFT JOIN LATERAL (
                SELECT COUNT(*) AS total_documents,
                       MAX(reference_date) AS last_document
                FROM documents WHERE company_id = c.id
            ) ds ON true
            {where}
            ORDER BY c.name ASC
            LIMIT %s OFFSET %s
            """,
            data_params,
        )
        rows = cur.fetchall()

    return {
        "data": [dict(r) for r in rows],  # type: ignore[arg-type]
        "total": total,
        "page": page,
        "per_page": per_page,
    }


@router.get("/{company_id}")
async def get_company(company_id: int) -> dict[str, Any]:
    """Get company details with current positions from latest document."""
    with get_cursor() as cur:
        cur.execute("SELECT * FROM companies WHERE id = %s", (company_id,))
        company = cur.fetchone()
        if not company:
            raise _not_found()

        # Get current positions from the latest document
        cur.execute(
            """
            SELECT h.asset_type, h.asset_description, h.quantity, h.total_value,
                   h.insider_group, h.confidence
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            WHERE d.company_id = %s
              AND d.id = (
                  SELECT id FROM documents
                  WHERE company_id = %s
                  ORDER BY reference_date DESC
                  LIMIT 1
              )
              AND h.section = 'final'
              AND h.confidence != 'baixa'
            ORDER BY h.insider_group, h.asset_type, h.asset_description
            """,
            (company_id, company_id),
        )
        positions = cur.fetchall()

    return {
        "data": {
            **dict(company),  # type: ignore[arg-type]
            "current_positions": [dict(p) for p in positions],  # type: ignore[arg-type]
        }
    }


@router.get("/{company_id}/documents")
async def get_company_documents(
    company_id: int,
    year: int | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get company documents."""
    offset = (page - 1) * per_page

    with get_cursor() as cur:
        # Verify company exists
        cur.execute("SELECT id FROM companies WHERE id = %s", (company_id,))
        if not cur.fetchone():
            raise _not_found()

        conditions = ["company_id = %s"]
        params: list[Any] = [company_id]

        if year is not None:
            conditions.append("year = %s")
            params.append(year)

        where = "WHERE " + " AND ".join(conditions)

        # Get total count
        cur.execute(f"SELECT COUNT(*) AS cnt FROM documents {where}", params)
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Get paginated results
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT * FROM documents {where}
            ORDER BY reference_date DESC
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


@router.get("/{company_id}/holdings")
async def get_company_holdings(
    company_id: int,
    section: str | None = None,
    asset_type: str | None = None,
    operation_type: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    insider_group: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
) -> dict[str, Any]:
    """Get company holdings."""
    validated_group = _validate_insider_group(insider_group)
    offset = (page - 1) * per_page

    with get_cursor() as cur:
        # Verify company exists
        cur.execute("SELECT id FROM companies WHERE id = %s", (company_id,))
        if not cur.fetchone():
            raise _not_found()

        conditions = ["d.company_id = %s"]
        params: list[Any] = [company_id]

        if section:
            conditions.append("h.section = %s")
            params.append(section)
        if asset_type:
            conditions.append("h.asset_type = %s")
            params.append(asset_type)
        if operation_type:
            conditions.append("h.operation_type = %s")
            params.append(operation_type)
        if date_from:
            conditions.append("d.reference_date >= %s")
            params.append(date_from)
        if date_to:
            conditions.append("d.reference_date <= %s")
            params.append(date_to)
        if validated_group:
            conditions.append("LOWER(h.insider_group) = LOWER(%s)")
            params.append(validated_group)

        where = "WHERE " + " AND ".join(conditions)

        # Count
        cur.execute(
            f"""
            SELECT COUNT(*) AS cnt FROM holdings h
            JOIN documents d ON d.id = h.document_id
            {where}
            """,
            params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Paginated results
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT h.*, d.reference_date
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            {where}
            ORDER BY d.reference_date DESC, h.id
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


@router.get("/{company_id}/position-history")
async def get_position_history(
    company_id: int,
    asset_type: str | None = None,
    months: int = Query(12, ge=1, le=60),
) -> dict[str, Any]:
    """Get position history time series."""
    with get_cursor() as cur:
        # Verify company exists
        cur.execute("SELECT id FROM companies WHERE id = %s", (company_id,))
        if not cur.fetchone():
            raise _not_found()

        conditions = ["company_id = %s"]
        params: list[Any] = [company_id]

        if asset_type:
            conditions.append("asset_type = %s")
            params.append(asset_type)

        params.append(months)
        where = "WHERE " + " AND ".join(conditions)

        cur.execute(
            f"""
            SELECT
                TO_CHAR(reference_date, 'YYYY-MM') AS month,
                SUM(posicao_inicial) AS posicao_inicial,
                SUM(posicao_final) AS posicao_final
            FROM v_monthly_positions
            {where}
              AND reference_date >= (CURRENT_DATE - INTERVAL '1 month' * %s)
            GROUP BY TO_CHAR(reference_date, 'YYYY-MM'), reference_date
            ORDER BY reference_date ASC
            """,
            params,
        )
        rows = cur.fetchall()

    return {"data": [dict(r) for r in rows]}  # type: ignore[arg-type]
