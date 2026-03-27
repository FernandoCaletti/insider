"""Rankings endpoints."""

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Query

from api.app.database import get_cursor
from api.app.routers.holdings import _validate_insider_group

router = APIRouter(prefix="/rankings", tags=["rankings"])

# Valid period values and their date offsets
_PERIOD_DAYS: dict[str, int | None] = {
    "7d": 7,
    "30d": 30,
    "90d": 90,
    "12m": 365,
    "all": None,
}


def _period_since(period: str) -> date | None:
    """Convert period string to a since-date, or None for 'all'."""
    days = _PERIOD_DAYS.get(period)
    if days is None:
        return None
    return date.today() - timedelta(days=days)


def _date_condition(since: date | None) -> tuple[str, list[Any]]:
    """Return SQL condition and params for date filtering."""
    if since is None:
        return "", []
    return "AND h.operation_date >= %s", [since]


def _group_condition(insider_group: str | None) -> tuple[str, list[Any]]:
    """Return SQL condition and params for insider_group filtering."""
    if insider_group is None:
        return "", []
    return "AND LOWER(h.insider_group) = LOWER(%s)", [insider_group]


@router.get("/top-buyers")
async def top_buyers(
    period: str = Query("30d", pattern="^(7d|30d|90d|12m|all)$"),
    insider_group: str | None = None,
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get top buyers ranking."""
    validated_group = _validate_insider_group(insider_group)
    since = _period_since(period)
    date_cond, date_params = _date_condition(since)
    group_cond, group_params = _group_condition(validated_group)

    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT
                c.id AS company_id,
                c.name AS company_name,
                c.ticker,
                COUNT(*) AS total_operations,
                COALESCE(SUM(h.total_value), 0) AS total_value,
                COALESCE(SUM(h.quantity), 0) AS total_quantity
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            JOIN companies c ON c.id = d.company_id
            WHERE h.section = 'movimentacoes'
              AND h.confidence != 'baixa'
              AND h.operation_type ILIKE 'Compra%%'
              {date_cond}
              {group_cond}
            GROUP BY c.id, c.name, c.ticker
            ORDER BY total_value DESC
            LIMIT %s
            """,
            [*date_params, *group_params, limit],
        )
        rows = cur.fetchall()

    return {"data": [dict(r) for r in rows], "period": period}  # type: ignore[arg-type]


@router.get("/top-sellers")
async def top_sellers(
    period: str = Query("30d", pattern="^(7d|30d|90d|12m|all)$"),
    insider_group: str | None = None,
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get top sellers ranking."""
    validated_group = _validate_insider_group(insider_group)
    since = _period_since(period)
    date_cond, date_params = _date_condition(since)
    group_cond, group_params = _group_condition(validated_group)

    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT
                c.id AS company_id,
                c.name AS company_name,
                c.ticker,
                COUNT(*) AS total_operations,
                COALESCE(SUM(ABS(h.total_value)), 0) AS total_value,
                COALESCE(SUM(h.quantity), 0) AS total_quantity
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            JOIN companies c ON c.id = d.company_id
            WHERE h.section = 'movimentacoes'
              AND h.confidence != 'baixa'
              AND h.operation_type ILIKE 'Venda%%'
              {date_cond}
              {group_cond}
            GROUP BY c.id, c.name, c.ticker
            ORDER BY total_value DESC
            LIMIT %s
            """,
            [*date_params, *group_params, limit],
        )
        rows = cur.fetchall()

    return {"data": [dict(r) for r in rows], "period": period}  # type: ignore[arg-type]


@router.get("/most-active")
async def most_active(
    period: str = Query("30d", pattern="^(7d|30d|90d|12m|all)$"),
    insider_group: str | None = None,
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get most active companies ranking."""
    validated_group = _validate_insider_group(insider_group)
    since = _period_since(period)
    date_cond, date_params = _date_condition(since)
    group_cond, group_params = _group_condition(validated_group)

    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT
                c.id AS company_id,
                c.name AS company_name,
                c.ticker,
                COUNT(*) AS total_operations,
                COALESCE(SUM(ABS(h.total_value)), 0) AS total_value
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            JOIN companies c ON c.id = d.company_id
            WHERE h.section = 'movimentacoes'
              AND h.confidence != 'baixa'
              {date_cond}
              {group_cond}
            GROUP BY c.id, c.name, c.ticker
            ORDER BY total_operations DESC, total_value DESC
            LIMIT %s
            """,
            [*date_params, *group_params, limit],
        )
        rows = cur.fetchall()

    return {"data": [dict(r) for r in rows], "period": period}  # type: ignore[arg-type]


@router.get("/largest-positions")
async def largest_positions(
    asset_type: str | None = None,
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get largest positions ranking (from latest document per company)."""
    asset_cond = ""
    params: list[Any] = []
    if asset_type:
        asset_cond = "AND h.asset_type = %s"
        params.append(asset_type)

    with get_cursor() as cur:
        cur.execute(
            f"""
            WITH latest_docs AS (
                SELECT DISTINCT ON (company_id)
                    id, company_id
                FROM documents
                ORDER BY company_id, reference_date DESC
            )
            SELECT
                c.id AS company_id,
                c.name AS company_name,
                c.ticker,
                h.asset_type,
                h.asset_description,
                COALESCE(SUM(h.quantity), 0) AS total_quantity,
                COALESCE(SUM(h.total_value), 0) AS estimated_value
            FROM holdings h
            JOIN latest_docs ld ON ld.id = h.document_id
            JOIN companies c ON c.id = ld.company_id
            WHERE h.section = 'final'
              AND h.confidence != 'baixa'
              {asset_cond}
            GROUP BY c.id, c.name, c.ticker, h.asset_type, h.asset_description
            ORDER BY estimated_value DESC
            LIMIT %s
            """,
            [*params, limit],
        )
        rows = cur.fetchall()

    return {"data": [dict(r) for r in rows]}  # type: ignore[arg-type]
