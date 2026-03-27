"""Dashboard endpoints."""

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Query

from api.app.database import get_cursor
from api.app.routers.holdings import _validate_insider_group

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def _mv_summary_exists(cur: Any) -> bool:
    """Check if mv_dashboard_summary materialized view exists."""
    cur.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_dashboard_summary')"
    )
    row = cur.fetchone()
    return bool(row["exists"]) if row else False  # type: ignore[index]


@router.get("/summary")
async def dashboard_summary() -> dict[str, Any]:
    """Get dashboard summary statistics."""
    with get_cursor() as cur:
        # Try materialized view first
        if _mv_summary_exists(cur):
            cur.execute("SELECT * FROM mv_dashboard_summary")
            mv_row = cur.fetchone()
            if mv_row:
                total_companies = mv_row["total_companies"]  # type: ignore[index]
                total_documents = mv_row["total_documents"]  # type: ignore[index]
                total_movements = mv_row["total_movements"]  # type: ignore[index]
                date_min = mv_row["date_min"]  # type: ignore[index]
                date_max = mv_row["date_max"]  # type: ignore[index]
            else:
                total_companies = total_documents = total_movements = 0
                date_min = date_max = None
        else:
            # Fallback to direct queries
            cur.execute("SELECT COUNT(*) AS cnt FROM companies")
            total_companies = cur.fetchone()["cnt"]  # type: ignore[index]

            cur.execute("SELECT COUNT(*) AS cnt FROM documents")
            total_documents = cur.fetchone()["cnt"]  # type: ignore[index]

            cur.execute(
                "SELECT COUNT(*) AS cnt FROM holdings WHERE section = 'movimentacoes' AND confidence != 'baixa'"
            )
            total_movements = cur.fetchone()["cnt"]  # type: ignore[index]

            cur.execute(
                "SELECT MIN(reference_date) AS date_min, MAX(reference_date) AS date_max FROM documents"
            )
            range_row = cur.fetchone()
            date_min = range_row["date_min"] if range_row else None  # type: ignore[index]
            date_max = range_row["date_max"] if range_row else None  # type: ignore[index]

        # Last sync (always live — it's a single row lookup with index)
        cur.execute(
            "SELECT id, started_at, finished_at, status, documents_found, documents_processed, documents_failed "
            "FROM sync_log ORDER BY started_at DESC LIMIT 1"
        )
        last_sync_row = cur.fetchone()
        last_sync = dict(last_sync_row) if last_sync_row else None  # type: ignore[arg-type]

        data_range = {
            "from": date_min.isoformat() if date_min else None,
            "to": date_max.isoformat() if date_max else None,
        }

    return {
        "data": {
            "total_companies": total_companies,
            "total_documents": total_documents,
            "total_movements": total_movements,
            "last_sync": last_sync,
            "data_range": data_range,
        }
    }


@router.get("/recent-movements")
async def recent_movements(
    days: int = Query(30, ge=1, le=365),
    insider_group: str | None = None,
    limit: int = Query(10, ge=1, le=100),
) -> dict[str, Any]:
    """Get top recent movements by value."""
    validated_group = _validate_insider_group(insider_group)
    since = date.today() - timedelta(days=days)

    group_cond = ""
    group_params: list[Any] = []
    if validated_group:
        group_cond = "AND LOWER(h.insider_group) = LOWER(%s)"
        group_params = [validated_group]

    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT
                h.id,
                c.id AS company_id,
                c.name AS company_name,
                c.ticker,
                h.asset_type,
                h.asset_description,
                h.operation_type,
                h.operation_date,
                h.quantity,
                h.unit_price,
                h.total_value,
                h.broker
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            JOIN companies c ON c.id = d.company_id
            WHERE h.section = 'movimentacoes'
              AND h.confidence != 'baixa'
              AND h.operation_date >= %s
              {group_cond}
            ORDER BY ABS(h.total_value) DESC NULLS LAST
            LIMIT %s
            """,
            [since, *group_params, limit],
        )
        rows = cur.fetchall()

    return {"data": [dict(r) for r in rows]}  # type: ignore[arg-type]


@router.get("/movements-chart")
async def movements_chart(
    days: int = Query(30, ge=1, le=365),
) -> dict[str, Any]:
    """Get movements chart time series."""
    since = date.today() - timedelta(days=days)
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                h.operation_date AS date,
                COUNT(*) FILTER (WHERE h.operation_type ILIKE 'Compra%%') AS compras,
                COUNT(*) FILTER (WHERE h.operation_type ILIKE 'Venda%%') AS vendas,
                COALESCE(SUM(h.total_value) FILTER (WHERE h.operation_type ILIKE 'Compra%%'), 0) AS valor_compras,
                COALESCE(SUM(ABS(h.total_value)) FILTER (WHERE h.operation_type ILIKE 'Venda%%'), 0) AS valor_vendas
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            WHERE h.section = 'movimentacoes'
              AND h.confidence != 'baixa'
              AND h.operation_date >= %s
              AND h.operation_date IS NOT NULL
            GROUP BY h.operation_date
            ORDER BY h.operation_date
            """,
            [since],
        )
        rows = cur.fetchall()

    return {"data": [dict(r) for r in rows]}  # type: ignore[arg-type]
