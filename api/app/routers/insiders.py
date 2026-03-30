"""Individual insider endpoints."""

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from api.app.database import get_cursor

router = APIRouter(prefix="/insiders", tags=["insiders"])


@router.get("")
async def list_insiders(
    search: str | None = None,
    insider_group: str | None = None,
    sort_by: str = Query("total_operations", pattern="^(insider_name|total_operations|total_value|companies_count)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
) -> dict[str, Any]:
    """List distinct insiders with aggregated stats from insider_positions."""
    conditions = ["ip.insider_name IS NOT NULL"]
    params: list[Any] = []

    if search:
        conditions.append("ip.insider_name ILIKE %s")
        params.append(f"%{search}%")
    if insider_group:
        conditions.append("LOWER(ip.insider_group) = LOWER(%s)")
        params.append(insider_group)

    # Filter out generic entries like 'Outros', 'Ações Tesouraria'
    conditions.append("ip.insider_name NOT IN ('Outros', 'Ações Tesouraria')")

    where = "WHERE " + " AND ".join(conditions)
    offset = (page - 1) * per_page

    sort_columns = {
        "insider_name": "insider_name",
        "total_operations": "total_operations",
        "total_value": "total_value",
        "companies_count": "companies_count",
    }
    sort_col = sort_columns.get(sort_by, "total_positions")
    order = "ASC" if sort_order == "asc" else "DESC"

    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT ip.insider_name
                FROM insider_positions ip
                {where}
                GROUP BY ip.insider_name
            ) sub
            """,
            params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        cur.execute(
            f"""
            SELECT
                ip.insider_name,
                COUNT(*) AS total_operations,
                COALESCE(SUM(ABS(ip.total_value)), 0) AS total_value,
                COUNT(DISTINCT ip.company_id) AS companies_count,
                MAX(ip.insider_group) AS insider_group,
                MIN(ip.reference_date) AS first_operation,
                MAX(ip.reference_date) AS last_operation,
                0 AS buy_count,
                0 AS sell_count
            FROM insider_positions ip
            {where}
            GROUP BY ip.insider_name
            ORDER BY {sort_col} {order}
            LIMIT %s OFFSET %s
            """,
            [*params, per_page, offset],
        )
        rows = cur.fetchall()

    return {
        "data": [dict(r) for r in rows],  # type: ignore[arg-type]
        "total": total,
        "page": page,
        "per_page": per_page,
    }


@router.get("/{insider_name}/summary")
async def get_insider_summary(insider_name: str) -> dict[str, Any]:
    """Get summary stats for a specific insider from insider_positions."""
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                ip.insider_name,
                COUNT(*) AS total_operations,
                COALESCE(SUM(ABS(ip.total_value)), 0) AS total_value,
                COUNT(DISTINCT ip.company_id) AS companies_count,
                MAX(ip.insider_group) AS insider_group,
                MIN(ip.reference_date) AS first_operation,
                MAX(ip.reference_date) AS last_operation,
                0 AS buy_count,
                0 AS sell_count,
                0 AS buy_value,
                0 AS sell_value
            FROM insider_positions ip
            WHERE ip.insider_name = %s
            GROUP BY ip.insider_name
            """,
            [insider_name],
        )
        row = cur.fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail="Insider não encontrado")

        # Get companies this insider has positions in
        cur.execute(
            """
            SELECT
                c.id AS company_id,
                c.name AS company_name,
                c.ticker AS company_ticker,
                COUNT(*) AS operations,
                COALESCE(SUM(ABS(ip.total_value)), 0) AS total_value
            FROM insider_positions ip
            JOIN companies c ON c.id = ip.company_id
            WHERE ip.insider_name = %s
            GROUP BY c.id, c.name, c.ticker
            ORDER BY total_value DESC
            """,
            [insider_name],
        )
        companies = cur.fetchall()

        # Alert count (may be zero if alerts table is empty)
        alert_count = 0
        try:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM alerts WHERE company_id IN (SELECT company_id FROM insider_positions WHERE insider_name = %s)",
                [insider_name],
            )
            alert_row = cur.fetchone()
            alert_count = alert_row["cnt"] if alert_row else 0  # type: ignore[index]
        except Exception:
            pass

    return {
        "data": {
            **dict(row),  # type: ignore[arg-type]
            "companies": [dict(c) for c in companies],  # type: ignore[arg-type]
            "alert_count": alert_count,
        }
    }


@router.get("/{insider_name}/holdings")
async def get_insider_holdings(
    insider_name: str,
    company_id: int | None = None,
    operation_type: str | None = None,
    asset_type: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    sort_by: str = Query("operation_date", pattern="^(operation_date|total_value|quantity|company_name|operation_type|asset_type)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
) -> dict[str, Any]:
    """Get paginated trading history for a specific insider."""
    conditions = [
        "h.insider_name = %s",
        "h.confidence != 'baixa'",
        "h.section = 'movimentacoes'",
    ]
    params: list[Any] = [insider_name]

    if company_id is not None:
        conditions.append("d.company_id = %s")
        params.append(company_id)
    if operation_type:
        conditions.append("h.operation_type ILIKE %s")
        params.append(f"{operation_type}%")
    if asset_type:
        conditions.append("h.asset_type = %s")
        params.append(asset_type)
    if date_from:
        conditions.append("h.operation_date >= %s")
        params.append(date_from)
    if date_to:
        conditions.append("h.operation_date <= %s")
        params.append(date_to)

    where = "WHERE " + " AND ".join(conditions)
    offset = (page - 1) * per_page

    sort_columns = {
        "operation_date": "h.operation_date",
        "total_value": "h.total_value",
        "quantity": "h.quantity",
        "company_name": "c.name",
        "operation_type": "h.operation_type",
        "asset_type": "h.asset_type",
    }
    sort_col = sort_columns.get(sort_by, "h.operation_date")
    order = "ASC" if sort_order == "asc" else "DESC"

    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            JOIN companies c ON c.id = d.company_id
            {where}
            """,
            params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        cur.execute(
            f"""
            SELECT
                h.*,
                d.reference_date,
                c.id AS company_id,
                c.name AS company_name,
                c.ticker AS company_ticker
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            JOIN companies c ON c.id = d.company_id
            {where}
            ORDER BY {sort_col} {order}, h.id
            LIMIT %s OFFSET %s
            """,
            [*params, per_page, offset],
        )
        rows = cur.fetchall()

    return {
        "data": [dict(r) for r in rows],  # type: ignore[arg-type]
        "total": total,
        "page": page,
        "per_page": per_page,
    }


@router.get("/{insider_name}/positions")
async def get_insider_positions(
    insider_name: str,
    company_id: int | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
) -> dict[str, Any]:
    """Get paginated position records for a specific insider."""
    conditions = ["ip.insider_name = %s"]
    params: list[Any] = [insider_name]

    if company_id is not None:
        conditions.append("ip.company_id = %s")
        params.append(company_id)
    if date_from:
        conditions.append("ip.reference_date >= %s")
        params.append(date_from)
    if date_to:
        conditions.append("ip.reference_date <= %s")
        params.append(date_to)

    where = "WHERE " + " AND ".join(conditions)
    offset = (page - 1) * per_page

    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM insider_positions ip
            {where}
            """,
            params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        cur.execute(
            f"""
            SELECT
                ip.*,
                c.name AS company_name,
                c.ticker AS company_ticker
            FROM insider_positions ip
            JOIN companies c ON c.id = ip.company_id
            {where}
            ORDER BY ip.reference_date DESC, ip.id
            LIMIT %s OFFSET %s
            """,
            [*params, per_page, offset],
        )
        rows = cur.fetchall()

    return {
        "data": [dict(r) for r in rows],  # type: ignore[arg-type]
        "total": total,
        "page": page,
        "per_page": per_page,
    }
