"""Rankings endpoints.

Uses materialized views (mv_rankings_*) for faster query performance.
Views are refreshed via POST /rankings/refresh after data syncs.
Falls back to direct queries if views don't exist yet.
"""

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

# All materialized view names used by rankings
_MV_NAMES = [
    "mv_rankings_top_buyers",
    "mv_rankings_top_sellers",
    "mv_rankings_most_active",
    "mv_rankings_by_role",
    "mv_rankings_by_broker",
    "mv_dashboard_summary",
]


def _period_since(period: str) -> date | None:
    """Convert period string to a since-date, or None for 'all'."""
    days = _PERIOD_DAYS.get(period)
    if days is None:
        return None
    return date.today() - timedelta(days=days)


def _date_condition(
    since: date | None, col: str = "operation_date"
) -> tuple[str, list[Any]]:
    """Return SQL condition and params for date filtering."""
    if since is None:
        return "", []
    return f"AND {col} >= %s", [since]


def _group_condition(
    insider_group: str | None, col: str = "insider_group"
) -> tuple[str, list[Any]]:
    """Return SQL condition and params for insider_group filtering."""
    if insider_group is None:
        return "", []
    return f"AND LOWER({col}) = LOWER(%s)", [insider_group]


def _mv_exists(cur: Any, mv_name: str) -> bool:
    """Check if a materialized view exists."""
    cur.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_matviews WHERE matviewname = %s)",
        [mv_name],
    )
    row = cur.fetchone()
    return bool(row["exists"]) if row else False  # type: ignore[index]


@router.post("/refresh")
async def refresh_materialized_views() -> dict[str, Any]:
    """Refresh all ranking materialized views. Call after data syncs."""
    refreshed: list[str] = []
    with get_cursor() as cur:
        for mv_name in _MV_NAMES:
            if _mv_exists(cur, mv_name):
                cur.execute(f"REFRESH MATERIALIZED VIEW {mv_name}")  # noqa: S608
                refreshed.append(mv_name)
    return {"refreshed": refreshed, "count": len(refreshed)}


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
        if _mv_exists(cur, "mv_rankings_top_buyers"):
            cur.execute(
                f"""
                SELECT
                    company_id,
                    company_name,
                    ticker,
                    SUM(op_count) AS total_operations,
                    SUM(total_value) AS total_value,
                    SUM(total_quantity) AS total_quantity
                FROM mv_rankings_top_buyers
                WHERE TRUE
                  {date_cond}
                  {group_cond}
                GROUP BY company_id, company_name, ticker
                ORDER BY total_value DESC
                LIMIT %s
                """,
                [*date_params, *group_params, limit],
            )
        else:
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
        if _mv_exists(cur, "mv_rankings_top_sellers"):
            cur.execute(
                f"""
                SELECT
                    company_id,
                    company_name,
                    ticker,
                    SUM(op_count) AS total_operations,
                    SUM(total_value) AS total_value,
                    SUM(total_quantity) AS total_quantity
                FROM mv_rankings_top_sellers
                WHERE TRUE
                  {date_cond}
                  {group_cond}
                GROUP BY company_id, company_name, ticker
                ORDER BY total_value DESC
                LIMIT %s
                """,
                [*date_params, *group_params, limit],
            )
        else:
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
        if _mv_exists(cur, "mv_rankings_most_active"):
            cur.execute(
                f"""
                SELECT
                    company_id,
                    company_name,
                    ticker,
                    SUM(op_count) AS total_operations,
                    SUM(total_value) AS total_value
                FROM mv_rankings_most_active
                WHERE TRUE
                  {date_cond}
                  {group_cond}
                GROUP BY company_id, company_name, ticker
                ORDER BY total_operations DESC, total_value DESC
                LIMIT %s
                """,
                [*date_params, *group_params, limit],
            )
        else:
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


@router.get("/by-role")
async def by_role(
    period: str = Query("30d", pattern="^(7d|30d|90d|12m|all)$"),
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get rankings grouped by insider role (insider_group)."""
    since = _period_since(period)
    date_cond, date_params = _date_condition(since)

    with get_cursor() as cur:
        if _mv_exists(cur, "mv_rankings_by_role"):
            cur.execute(
                f"""
                SELECT
                    insider_group,
                    SUM(op_count) AS total_operations,
                    SUM(total_value) AS total_value,
                    SUM(companies_count) AS companies_count,
                    SUM(buy_count) AS buy_count,
                    SUM(sell_count) AS sell_count
                FROM mv_rankings_by_role
                WHERE TRUE
                  {date_cond}
                GROUP BY insider_group
                ORDER BY total_value DESC
                LIMIT %s
                """,
                [*date_params, limit],
            )
        else:
            cur.execute(
                f"""
                SELECT
                    h.insider_group,
                    COUNT(*) AS total_operations,
                    COALESCE(SUM(ABS(h.total_value)), 0) AS total_value,
                    COUNT(DISTINCT d.company_id) AS companies_count,
                    COALESCE(SUM(CASE WHEN h.operation_type ILIKE 'Compra%%' THEN 1 ELSE 0 END), 0) AS buy_count,
                    COALESCE(SUM(CASE WHEN h.operation_type ILIKE 'Venda%%' THEN 1 ELSE 0 END), 0) AS sell_count
                FROM holdings h
                JOIN documents d ON d.id = h.document_id
                WHERE h.section = 'movimentacoes'
                  AND h.confidence != 'baixa'
                  AND h.insider_group IS NOT NULL
                  {date_cond}
                GROUP BY h.insider_group
                ORDER BY total_value DESC
                LIMIT %s
                """,
                [*date_params, limit],
            )
        rows = cur.fetchall()

    return {"data": [dict(r) for r in rows], "period": period}  # type: ignore[arg-type]


@router.get("/by-broker")
async def by_broker(
    period: str = Query("30d", pattern="^(7d|30d|90d|12m|all)$"),
    insider_group: str | None = None,
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get rankings grouped by broker."""
    validated_group = _validate_insider_group(insider_group)
    since = _period_since(period)
    date_cond, date_params = _date_condition(since)
    group_cond, group_params = _group_condition(validated_group)

    with get_cursor() as cur:
        if _mv_exists(cur, "mv_rankings_by_broker"):
            cur.execute(
                f"""
                SELECT
                    broker,
                    SUM(op_count) AS total_operations,
                    SUM(total_value) AS total_value,
                    SUM(companies_count) AS companies_count,
                    SUM(buy_count) AS buy_count,
                    SUM(sell_count) AS sell_count
                FROM mv_rankings_by_broker
                WHERE TRUE
                  {date_cond}
                  {group_cond}
                GROUP BY broker
                ORDER BY total_value DESC
                LIMIT %s
                """,
                [*date_params, *group_params, limit],
            )
        else:
            cur.execute(
                f"""
                SELECT
                    h.broker,
                    COUNT(*) AS total_operations,
                    COALESCE(SUM(ABS(h.total_value)), 0) AS total_value,
                    COUNT(DISTINCT d.company_id) AS companies_count,
                    COALESCE(SUM(CASE WHEN h.operation_type ILIKE 'Compra%%' THEN 1 ELSE 0 END), 0) AS buy_count,
                    COALESCE(SUM(CASE WHEN h.operation_type ILIKE 'Venda%%' THEN 1 ELSE 0 END), 0) AS sell_count
                FROM holdings h
                JOIN documents d ON d.id = h.document_id
                WHERE h.section = 'movimentacoes'
                  AND h.confidence != 'baixa'
                  AND h.broker IS NOT NULL
                  AND h.broker != ''
                  {date_cond}
                  {group_cond}
                GROUP BY h.broker
                ORDER BY total_value DESC
                LIMIT %s
                """,
                [*date_params, *group_params, limit],
            )
        rows = cur.fetchall()

    return {"data": [dict(r) for r in rows], "period": period}  # type: ignore[arg-type]


@router.get("/by-alerts")
async def by_alerts(
    period: str = Query("30d", pattern="^(7d|30d|90d|12m|all)$"),
    alert_type: str | None = None,
    severity: str | None = None,
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get companies ranked by alert count."""
    since = _period_since(period)
    conditions: list[str] = []
    params: list[Any] = []

    if since is not None:
        conditions.append("a.created_at >= %s")
        params.append(since)
    if alert_type:
        conditions.append("a.alert_type = %s")
        params.append(alert_type)
    if severity:
        conditions.append("a.severity = %s")
        params.append(severity)

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT
                c.id AS company_id,
                c.name AS company_name,
                c.ticker,
                COUNT(*) AS alert_count,
                COALESCE(SUM(CASE WHEN a.severity = 'critical' THEN 1 ELSE 0 END), 0) AS critical_count,
                COALESCE(SUM(CASE WHEN a.severity = 'high' THEN 1 ELSE 0 END), 0) AS high_count,
                COALESCE(SUM(CASE WHEN a.is_read = FALSE THEN 1 ELSE 0 END), 0) AS unread_count
            FROM alerts a
            JOIN companies c ON c.id = a.company_id
            {where}
            GROUP BY c.id, c.name, c.ticker
            ORDER BY alert_count DESC
            LIMIT %s
            """,
            [*params, limit],
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
