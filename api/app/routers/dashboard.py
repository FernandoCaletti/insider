"""Dashboard endpoints."""

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Query

from api.app.database import get_cursor
from api.app.routers.holdings import _validate_insider_group

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

_PERIOD_DAYS = {
    "30d": 30,
    "90d": 90,
    "12m": 365,
}


def _mv_summary_exists(cur: Any) -> bool:
    """Check if mv_dashboard_summary materialized view exists."""
    cur.execute(
        "SELECT EXISTS(SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_dashboard_summary')"
    )
    row = cur.fetchone()
    return bool(row["exists"]) if row else False  # type: ignore[index]


def _op_verb(operation_type: str | None) -> str:
    """Convert operation_type to a verb for display."""
    if not operation_type:
        return "operou"
    lower = operation_type.lower()
    if "compra" in lower:
        return "comprou"
    if "venda" in lower:
        return "vendeu"
    return "operou"


def _fmt_value(v: float | None) -> str:
    """Format a value as R$ X.XXX.XXX."""
    if v is None:
        return "R$ 0"
    abs_v = abs(v)
    if abs_v >= 1_000_000:
        return f"R$ {abs_v / 1_000_000:,.1f}M".replace(",", ".")
    if abs_v >= 1_000:
        return f"R$ {abs_v / 1_000:,.0f}K".replace(",", ".")
    return f"R$ {abs_v:,.0f}"


def _safe_pct_change(current: float, previous: float) -> float:
    """Calculate percentage change safely."""
    if previous == 0:
        return 0.0 if current == 0 else 100.0
    return round(((current - previous) / abs(previous)) * 100, 1)


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

        # Last sync (always live -- it's a single row lookup with index)
        cur.execute(
            "SELECT id, started_at, finished_at, status, documents_found, documents_processed, documents_failed "
            "FROM sync_log ORDER BY started_at DESC LIMIT 1"
        )
        last_sync_row = cur.fetchone()
        last_sync = dict(last_sync_row) if last_sync_row else None  # type: ignore[arg-type]
        last_sync_docs = (last_sync_row["documents_processed"] if last_sync_row else 0) or 0  # type: ignore[index]

        data_range = {
            "from": date_min.isoformat() if date_min else None,
            "to": date_max.isoformat() if date_max else None,
        }

        # New companies this month
        today = date.today()
        first_of_month = today.replace(day=1)
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM companies WHERE created_at >= %s",
            [first_of_month],
        )
        row = cur.fetchone()
        new_companies_this_month = row["cnt"] if row else 0  # type: ignore[index]

        # Movements 90d and previous 90d
        since_30d = today - timedelta(days=90)
        since_60d = today - timedelta(days=180)

        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE h.operation_date >= %s) AS movements_30d,
                COUNT(*) FILTER (WHERE h.operation_date >= %s AND h.operation_date < %s) AS movements_prev_30d,
                COALESCE(SUM(h.total_value) FILTER (
                    WHERE h.operation_date >= %s AND h.operation_type ILIKE 'Compra%%'
                ), 0) -
                COALESCE(SUM(ABS(h.total_value)) FILTER (
                    WHERE h.operation_date >= %s AND h.operation_type ILIKE 'Venda%%'
                ), 0) AS balance_30d,
                COALESCE(SUM(h.total_value) FILTER (
                    WHERE h.operation_date >= %s AND h.operation_date < %s AND h.operation_type ILIKE 'Compra%%'
                ), 0) -
                COALESCE(SUM(ABS(h.total_value)) FILTER (
                    WHERE h.operation_date >= %s AND h.operation_date < %s AND h.operation_type ILIKE 'Venda%%'
                ), 0) AS balance_previous_30d
            FROM holdings h
            WHERE h.section = 'movimentacoes'
              AND h.confidence != 'baixa'
              AND h.operation_date >= %s
            """,
            [
                since_30d,
                since_60d, since_30d,
                since_30d,
                since_30d,
                since_60d, since_30d,
                since_60d, since_30d,
                since_60d,
            ],
        )
        mv_row = cur.fetchone()
        movements_30d = mv_row["movements_30d"] if mv_row else 0  # type: ignore[index]
        movements_prev_30d = mv_row["movements_prev_30d"] if mv_row else 0  # type: ignore[index]
        balance_30d = float(mv_row["balance_30d"]) if mv_row and mv_row["balance_30d"] else 0.0  # type: ignore[index]
        balance_previous_30d = float(mv_row["balance_previous_30d"]) if mv_row and mv_row["balance_previous_30d"] else 0.0  # type: ignore[index]

        movements_30d_change_pct = _safe_pct_change(movements_30d, movements_prev_30d)
        balance_change_pct = _safe_pct_change(balance_30d, balance_previous_30d)

    return {
        "data": {
            "total_companies": total_companies,
            "total_documents": total_documents,
            "total_movements": total_movements,
            "last_sync": last_sync,
            "data_range": data_range,
            "new_companies_this_month": new_companies_this_month,
            "last_sync_docs": last_sync_docs,
            "movements_30d": movements_30d,
            "movements_30d_change_pct": movements_30d_change_pct,
            "balance_30d": balance_30d,
            "balance_previous_30d": balance_previous_30d,
            "balance_change_pct": balance_change_pct,
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
                h.broker,
                h.insider_group
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
    period: str | None = Query(None, description="Period: 30d, 90d, 12m"),
) -> dict[str, Any]:
    """Get movements chart time series."""
    # Period param overrides days
    if period and period in _PERIOD_DAYS:
        actual_days = _PERIOD_DAYS[period]
    else:
        actual_days = days

    since = date.today() - timedelta(days=actual_days)
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

    data_points = []
    for r in rows:
        row_dict = dict(r)  # type: ignore[arg-type]
        valor_compras = float(row_dict.get("valor_compras") or 0)
        valor_vendas = float(row_dict.get("valor_vendas") or 0)
        row_dict["saldo"] = valor_compras - valor_vendas
        data_points.append(row_dict)

    # Calculate 7-day moving average of saldo
    moving_average_7d = []
    for i, dp in enumerate(data_points):
        window = data_points[max(0, i - 6):i + 1]
        avg_saldo = sum(float(w.get("saldo", 0)) for w in window) / len(window)
        moving_average_7d.append({
            "date": dp["date"],
            "saldo_ma7": round(avg_saldo, 2),
        })

    return {
        "data": data_points,
        "moving_average_7d": moving_average_7d,
    }


@router.get("/hero-insight")
async def hero_insight() -> dict[str, Any]:
    """Get the most relevant insight (last 90 days)."""
    today = date.today()
    since_7d = today - timedelta(days=90)

    with get_cursor() as cur:
        # Priority 1: Correlation - insider movement within 15 days before a material fact
        cur.execute(
            """
            SELECT
                h.id AS holding_id,
                h.insider_group,
                h.operation_type,
                h.asset_type,
                h.total_value,
                h.quantity,
                h.operation_date,
                c.id AS company_id,
                c.name AS company_name,
                c.ticker,
                mf.subject AS fact_title,
                mf.reference_date AS fact_date,
                (mf.reference_date - h.operation_date) AS days_before
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            JOIN companies c ON c.id = d.company_id
            JOIN material_facts mf ON mf.company_id = c.id
                AND mf.reference_date > h.operation_date
                AND mf.reference_date <= h.operation_date + INTERVAL '15 days'
            WHERE h.section = 'movimentacoes'
              AND h.confidence != 'baixa'
              AND h.operation_date >= %s
            ORDER BY ABS(h.total_value) DESC NULLS LAST
            LIMIT 1
            """,
            [since_7d],
        )
        corr_row = cur.fetchone()

        if corr_row:
            raw_ticker = corr_row["ticker"]  # type: ignore[index]
            ticker = raw_ticker or corr_row["company_name"]  # type: ignore[index]
            insider_group = corr_row["insider_group"] or "Insider"  # type: ignore[index]
            op_verb = _op_verb(corr_row["operation_type"])  # type: ignore[index]
            valor = _fmt_value(float(corr_row["total_value"])) if corr_row["total_value"] else "R$ 0"  # type: ignore[index]
            asset_map = {"ACAO_ON": "Ações ON", "ACAO_PN": "Ações PN", "DEBENTURE": "Debêntures", "OPCAO": "Opções", "OPCAO_COMPRA": "Opções de Compra", "OPCAO_VENDA": "Opções de Venda", "BDR": "BDRs", "UNIT": "Units"}
            asset = asset_map.get(corr_row["asset_type"], corr_row["asset_type"] or "ativos")  # type: ignore[index]
            raw_days = corr_row["days_before"]  # type: ignore[index]
            days_before = raw_days.days if hasattr(raw_days, "days") else int(raw_days or 0)
            fact_title = corr_row["fact_title"] or ""  # type: ignore[index]

            title = (
                f"{insider_group} da {ticker} {op_verb} {valor} em {asset} "
                f"— {days_before} dias antes de: {fact_title}"
            )
            return {
                "data": {
                    "type": "correlation",
                    "title": title,
                    "subtitle": f"Movimentacao correlacionada com fato relevante",
                    "company": {
                        "id": corr_row["company_id"],  # type: ignore[index]
                        "name": corr_row["company_name"],  # type: ignore[index]
                        "ticker": raw_ticker,
                    },
                    "badges": {
                        "alert_type": "correlation",
                        "severity": "alta",
                        "insider_group": insider_group,
                        "operation_type": corr_row["operation_type"],  # type: ignore[index]
                    },
                    "values": {
                        "total_value": float(corr_row["total_value"]) if corr_row["total_value"] else 0,  # type: ignore[index]
                        "quantity": corr_row["quantity"],  # type: ignore[index]
                        "operation_date": corr_row["operation_date"].isoformat() if corr_row["operation_date"] else None,  # type: ignore[index]
                    },
                    "correlation": {
                        "fact_title": fact_title,
                        "fact_date": corr_row["fact_date"].isoformat() if corr_row["fact_date"] else None,  # type: ignore[index]
                        "days_before": days_before,
                    },
                }
            }

        # Priority 2 & 3: High or medium severity alert in last 7 days
        cur.execute(
            """
            SELECT
                a.id AS alert_id,
                a.alert_type,
                a.severity,
                a.title AS alert_title,
                a.description,
                a.created_at,
                c.id AS company_id,
                c.name AS company_name,
                c.ticker,
                h.insider_group,
                h.operation_type,
                h.asset_type,
                h.total_value,
                h.quantity,
                h.operation_date
            FROM alerts a
            JOIN companies c ON c.id = a.company_id
            LEFT JOIN holdings h ON h.id = a.holding_id
            WHERE a.created_at >= %s
              AND a.severity IN ('high', 'critical', 'medium')
            ORDER BY
                CASE a.severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                a.created_at DESC
            LIMIT 1
            """,
            [since_7d],
        )
        alert_row = cur.fetchone()

        if alert_row:
            ticker = alert_row["ticker"] or ""  # type: ignore[index]
            insider_group = alert_row["insider_group"] or "Insider"  # type: ignore[index]
            alert_type = alert_row["alert_type"] or ""  # type: ignore[index]
            severity = alert_row["severity"] or ""  # type: ignore[index]
            total_value = float(alert_row["total_value"]) if alert_row["total_value"] else 0  # type: ignore[index]
            valor = _fmt_value(total_value)
            op_verb = _op_verb(alert_row["operation_type"])  # type: ignore[index]
            asset = alert_row["asset_type"] or "ativos"  # type: ignore[index]

            # Template based on alert_type
            if alert_type == "alto_valor":
                title = f"{insider_group} da {ticker} {op_verb} {valor} em {asset}"
            elif alert_type == "mudanca_direcao":
                title = f"{ticker}: insiders mudaram de direcao"
            elif alert_type == "volume_atipico":
                title = f"{ticker}: volume de operacoes atipico"
            elif alert_type == "retorno_atividade":
                title = f"{insider_group} da {ticker} volta a operar"
            else:
                title = alert_row["alert_title"] or f"Alerta: {ticker}"  # type: ignore[index]

            return {
                "data": {
                    "type": "alert",
                    "title": title,
                    "subtitle": alert_row["description"] or f"Alerta de severidade {severity}",  # type: ignore[index]
                    "company": {
                        "id": alert_row["company_id"],  # type: ignore[index]
                        "name": alert_row["company_name"],  # type: ignore[index]
                        "ticker": ticker,
                    },
                    "badges": {
                        "alert_type": alert_type,
                        "severity": severity,
                        "insider_group": insider_group,
                        "operation_type": alert_row["operation_type"],  # type: ignore[index]
                    },
                    "values": {
                        "total_value": total_value,
                        "quantity": alert_row["quantity"],  # type: ignore[index]
                        "operation_date": alert_row["operation_date"].isoformat() if alert_row["operation_date"] else None,  # type: ignore[index]
                    },
                    "correlation": None,
                }
            }

        # Priority 4: Fallback - largest movement by absolute value in last 7 days
        cur.execute(
            """
            SELECT
                h.insider_group,
                h.operation_type,
                h.asset_type,
                h.total_value,
                h.quantity,
                h.operation_date,
                c.id AS company_id,
                c.name AS company_name,
                c.ticker
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            JOIN companies c ON c.id = d.company_id
            WHERE h.section = 'movimentacoes'
              AND h.confidence != 'baixa'
              AND h.operation_date >= %s
            ORDER BY ABS(h.total_value) DESC NULLS LAST
            LIMIT 1
            """,
            [since_7d],
        )
        fallback_row = cur.fetchone()

        if fallback_row:
            ticker = fallback_row["ticker"] or ""  # type: ignore[index]
            insider_group = fallback_row["insider_group"] or "Insider"  # type: ignore[index]
            op_verb = _op_verb(fallback_row["operation_type"])  # type: ignore[index]
            total_value = float(fallback_row["total_value"]) if fallback_row["total_value"] else 0  # type: ignore[index]
            valor = _fmt_value(total_value)

            title = f"Maior movimentacao da semana: {insider_group} da {ticker} {op_verb} {valor}"
            return {
                "data": {
                    "type": "movement",
                    "title": title,
                    "subtitle": "Maior movimentacao dos ultimos 7 dias",
                    "company": {
                        "id": fallback_row["company_id"],  # type: ignore[index]
                        "name": fallback_row["company_name"],  # type: ignore[index]
                        "ticker": ticker,
                    },
                    "badges": {
                        "alert_type": None,
                        "severity": None,
                        "insider_group": insider_group,
                        "operation_type": fallback_row["operation_type"],  # type: ignore[index]
                    },
                    "values": {
                        "total_value": total_value,
                        "quantity": fallback_row["quantity"],  # type: ignore[index]
                        "operation_date": fallback_row["operation_date"].isoformat() if fallback_row["operation_date"] else None,  # type: ignore[index]
                    },
                    "correlation": None,
                }
            }

    # No data at all
    return {
        "data": {
            "type": "movement",
            "title": "Sem movimentacoes recentes",
            "subtitle": "Nenhuma movimentacao encontrada nos ultimos 7 dias",
            "company": None,
            "badges": {},
            "values": {},
            "correlation": None,
        }
    }


@router.get("/market-temperature")
async def market_temperature() -> dict[str, Any]:
    """Aggregate buy vs sell activity over 90 days, compared with previous 90 days."""
    today = date.today()
    since_30d = today - timedelta(days=90)
    since_60d = today - timedelta(days=180)

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                COALESCE(SUM(h.total_value) FILTER (
                    WHERE h.operation_date >= %s AND h.operation_type ILIKE 'Compra%%'
                ), 0) AS total_buys,
                COALESCE(SUM(ABS(h.total_value)) FILTER (
                    WHERE h.operation_date >= %s AND h.operation_type ILIKE 'Venda%%'
                ), 0) AS total_sells,
                COUNT(*) FILTER (
                    WHERE h.operation_date >= %s AND h.operation_type ILIKE 'Compra%%'
                ) AS buys_count,
                COUNT(*) FILTER (
                    WHERE h.operation_date >= %s AND h.operation_type ILIKE 'Venda%%'
                ) AS sells_count,
                COALESCE(SUM(h.total_value) FILTER (
                    WHERE h.operation_date >= %s AND h.operation_date < %s AND h.operation_type ILIKE 'Compra%%'
                ), 0) AS prev_buys,
                COALESCE(SUM(ABS(h.total_value)) FILTER (
                    WHERE h.operation_date >= %s AND h.operation_date < %s AND h.operation_type ILIKE 'Venda%%'
                ), 0) AS prev_sells
            FROM holdings h
            WHERE h.section = 'movimentacoes'
              AND h.confidence != 'baixa'
              AND h.operation_date >= %s
            """,
            [
                since_30d,
                since_30d,
                since_30d,
                since_30d,
                since_60d, since_30d,
                since_60d, since_30d,
                since_60d,
            ],
        )
        row = cur.fetchone()

    total_buys = float(row["total_buys"]) if row and row["total_buys"] else 0.0  # type: ignore[index]
    total_sells = float(row["total_sells"]) if row and row["total_sells"] else 0.0  # type: ignore[index]
    buys_count = row["buys_count"] if row else 0  # type: ignore[index]
    sells_count = row["sells_count"] if row else 0  # type: ignore[index]
    prev_buys = float(row["prev_buys"]) if row and row["prev_buys"] else 0.0  # type: ignore[index]
    prev_sells = float(row["prev_sells"]) if row and row["prev_sells"] else 0.0  # type: ignore[index]

    balance = total_buys - total_sells
    total = total_buys + total_sells
    ratio = round(total_buys / total, 3) if total > 0 else 0.5

    if ratio > 0.6:
        label = "Insiders estao COMPRANDO mais"
        sentiment = "buying"
    elif ratio < 0.4:
        label = "Insiders estao VENDENDO mais"
        sentiment = "selling"
    else:
        label = "Mercado EQUILIBRADO"
        sentiment = "neutral"

    prev_balance = prev_buys - prev_sells

    return {
        "data": {
            "period_days": 30,
            "total_buys": total_buys,
            "total_sells": total_sells,
            "balance": balance,
            "ratio": ratio,
            "label": label,
            "sentiment": sentiment,
            "operations_count": {
                "buys": buys_count,
                "sells": sells_count,
            },
            "vs_previous_period": {
                "buys_change_pct": _safe_pct_change(total_buys, prev_buys),
                "sells_change_pct": _safe_pct_change(total_sells, prev_sells),
                "balance_change_pct": _safe_pct_change(balance, prev_balance),
            },
        }
    }


@router.get("/activity-radar")
async def activity_radar(
    limit: int = Query(5, ge=1, le=20),
) -> dict[str, Any]:
    """Top companies with atypical activity."""
    today = date.today()
    since_1m = today - timedelta(days=90)
    since_6m = today - timedelta(days=365)

    with get_cursor() as cur:
        # Get companies with current month ops and 6-month average
        cur.execute(
            """
            WITH current_month AS (
                SELECT
                    c.id AS company_id,
                    c.name AS company_name,
                    c.ticker,
                    COUNT(*) AS current_ops,
                    COALESCE(SUM(h.total_value) FILTER (WHERE h.operation_type ILIKE 'Compra%%'), 0) AS buy_value,
                    COALESCE(SUM(ABS(h.total_value)) FILTER (WHERE h.operation_type ILIKE 'Venda%%'), 0) AS sell_value,
                    COALESCE(SUM(ABS(h.total_value)), 0) AS total_value,
                    MODE() WITHIN GROUP (ORDER BY h.operation_type) AS dominant_operation,
                    MODE() WITHIN GROUP (ORDER BY h.insider_group) AS insider_group
                FROM holdings h
                JOIN documents d ON d.id = h.document_id
                JOIN companies c ON c.id = d.company_id
                WHERE h.section = 'movimentacoes'
                  AND h.confidence != 'baixa'
                  AND h.operation_date >= %s
                  AND (h.operation_type ILIKE 'Compra%%' OR h.operation_type ILIKE 'Venda%%')
                GROUP BY c.id, c.name, c.ticker
            ),
            historical AS (
                SELECT
                    d.company_id,
                    COUNT(*) / GREATEST(6.0, 1.0) AS avg_monthly_ops
                FROM holdings h
                JOIN documents d ON d.id = h.document_id
                WHERE h.section = 'movimentacoes'
                  AND h.confidence != 'baixa'
                  AND h.operation_date >= %s
                  AND h.operation_date < %s
                  AND (h.operation_type ILIKE 'Compra%%' OR h.operation_type ILIKE 'Venda%%')
                GROUP BY d.company_id
            )
            SELECT
                cm.*,
                COALESCE(hi.avg_monthly_ops, 0) AS avg_monthly_ops,
                CASE
                    WHEN COALESCE(hi.avg_monthly_ops, 0) > 0
                    THEN ROUND(cm.current_ops::numeric / hi.avg_monthly_ops, 2)
                    ELSE cm.current_ops::numeric
                END AS multiplier
            FROM current_month cm
            LEFT JOIN historical hi ON hi.company_id = cm.company_id
            ORDER BY
                CASE
                    WHEN COALESCE(hi.avg_monthly_ops, 0) > 0
                    THEN cm.current_ops::numeric / hi.avg_monthly_ops
                    ELSE cm.current_ops::numeric
                END DESC
            """,
            [since_1m, since_6m, since_1m],
        )
        rows = cur.fetchall()

        # Separate atypical (multiplier > 1.5) and fallback
        atypical = []
        fallback = []
        for r in rows:
            r_dict = dict(r)  # type: ignore[arg-type]
            if float(r_dict.get("multiplier", 0)) > 1.5:
                atypical.append(r_dict)
            else:
                fallback.append(r_dict)

        # Fill with highest value companies if not enough atypical
        result_rows = atypical[:limit]
        if len(result_rows) < limit:
            # Sort fallback by total_value desc
            fallback.sort(key=lambda x: float(x.get("total_value", 0)), reverse=True)
            result_rows.extend(fallback[:limit - len(result_rows)])

        # Check for alerts and correlations for each company
        company_ids = [r["company_id"] for r in result_rows]
        alerts_map: dict[int, dict[str, Any]] = {}
        correlations_set: set[int] = set()

        if company_ids:
            placeholders = ",".join(["%s"] * len(company_ids))
            cur.execute(
                f"""
                SELECT DISTINCT ON (company_id)
                    company_id, severity, alert_type
                FROM alerts
                WHERE company_id IN ({placeholders})
                  AND created_at >= %s
                ORDER BY company_id,
                    CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END
                """,
                [*company_ids, since_1m],
            )
            for ar in cur.fetchall():
                alerts_map[ar["company_id"]] = {  # type: ignore[index]
                    "severity": ar["severity"],  # type: ignore[index]
                    "alert_type": ar["alert_type"],  # type: ignore[index]
                }

            cur.execute(
                f"""
                SELECT DISTINCT c.id AS company_id
                FROM companies c
                JOIN documents d ON d.company_id = c.id
                JOIN holdings h ON h.document_id = d.id
                JOIN material_facts mf ON mf.company_id = c.id
                    AND mf.reference_date > h.operation_date
                    AND mf.reference_date <= h.operation_date + INTERVAL '15 days'
                WHERE c.id IN ({placeholders})
                  AND h.section = 'movimentacoes'
                  AND h.confidence != 'baixa'
                  AND h.operation_date >= %s
                """,
                [*company_ids, since_1m],
            )
            for cr in cur.fetchall():
                correlations_set.add(cr["company_id"])  # type: ignore[index]

    data = []
    for r in result_rows:
        cid = r["company_id"]
        buy_val = float(r.get("buy_value", 0))
        sell_val = float(r.get("sell_value", 0))
        direction = "buying" if buy_val >= sell_val else "selling"
        alert_info = alerts_map.get(cid, {})

        data.append({
            "company_id": cid,
            "company_name": r["company_name"],
            "ticker": r["ticker"],
            "direction": direction,
            "multiplier": float(r.get("multiplier", 0)),
            "dominant_operation": r.get("dominant_operation"),
            "total_value": float(r.get("total_value", 0)),
            "operations_count": r.get("current_ops", 0),
            "insider_group": r.get("insider_group"),
            "alert_severity": alert_info.get("severity"),
            "alert_type": alert_info.get("alert_type"),
            "has_correlation": cid in correlations_set,
        })

    return {"data": data}
