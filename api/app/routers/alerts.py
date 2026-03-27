"""Alerts endpoints."""

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from api.app.database import get_cursor

router = APIRouter(prefix="/alerts", tags=["alerts"])

_VALID_ALERT_TYPES = {"alto_valor", "volume_atipico", "mudanca_direcao", "retorno_atividade"}
_VALID_SEVERITIES = {"low", "medium", "high", "critical"}


@router.get("/summary")
async def alerts_summary() -> dict[str, Any]:
    """Get alert counts grouped by type and severity, plus unread count."""
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT alert_type,
                   COUNT(*) AS count
            FROM alerts
            GROUP BY alert_type
            ORDER BY count DESC
            """
        )
        by_type = [dict(r) for r in cur.fetchall()]  # type: ignore[arg-type]

        cur.execute(
            """
            SELECT severity,
                   COUNT(*) AS count
            FROM alerts
            GROUP BY severity
            ORDER BY CASE severity
                WHEN 'critical' THEN 1
                WHEN 'high' THEN 2
                WHEN 'medium' THEN 3
                WHEN 'low' THEN 4
            END
            """
        )
        by_severity = [dict(r) for r in cur.fetchall()]  # type: ignore[arg-type]

        cur.execute("SELECT COUNT(*) AS cnt FROM alerts WHERE is_read = FALSE")
        unread = cur.fetchone()["cnt"]  # type: ignore[index]

        cur.execute("SELECT COUNT(*) AS cnt FROM alerts")
        total = cur.fetchone()["cnt"]  # type: ignore[index]

    return {
        "data": {
            "total": total,
            "unread": unread,
            "by_type": by_type,
            "by_severity": by_severity,
        }
    }


@router.get("")
async def list_alerts(
    company_id: int | None = None,
    alert_type: str | None = None,
    severity: str | None = None,
    is_read: bool | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    search: str | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """List alerts with optional filters."""
    if alert_type and alert_type not in _VALID_ALERT_TYPES:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_ALERT_TYPE",
                "message": f"Tipo invalido. Valores aceitos: {', '.join(sorted(_VALID_ALERT_TYPES))}",
            },
        )
    if severity and severity not in _VALID_SEVERITIES:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_SEVERITY",
                "message": f"Severidade invalida. Valores aceitos: {', '.join(sorted(_VALID_SEVERITIES))}",
            },
        )

    offset = (page - 1) * per_page

    with get_cursor() as cur:
        conditions: list[str] = []
        params: list[Any] = []

        if company_id is not None:
            conditions.append("a.company_id = %s")
            params.append(company_id)

        if alert_type:
            conditions.append("a.alert_type = %s")
            params.append(alert_type)

        if severity:
            conditions.append("a.severity = %s")
            params.append(severity)

        if is_read is not None:
            conditions.append("a.is_read = %s")
            params.append(is_read)

        if date_from is not None:
            conditions.append("a.created_at >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("a.created_at < %s::date + INTERVAL '1 day'")
            params.append(date_to)

        if search:
            conditions.append(
                "(a.title ILIKE %s OR c.name ILIKE %s OR c.ticker ILIKE %s)"
            )
            params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count
        count_params = list(params)
        cur.execute(
            f"""
            SELECT COUNT(*) AS cnt
            FROM alerts a
            JOIN companies c ON c.id = a.company_id
            {where}
            """,
            count_params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Paginated results
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT a.id,
                   a.company_id,
                   a.holding_id,
                   a.alert_type,
                   a.severity,
                   a.title,
                   a.description,
                   a.metadata,
                   a.is_read,
                   a.created_at,
                   c.name AS company_name,
                   c.ticker AS company_ticker
            FROM alerts a
            JOIN companies c ON c.id = a.company_id
            {where}
            ORDER BY a.created_at DESC, a.id DESC
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


@router.get("/{alert_id}")
async def get_alert(alert_id: int) -> dict[str, Any]:
    """Get a single alert with company info."""
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT a.*,
                   c.name AS company_name,
                   c.ticker AS company_ticker,
                   c.cvm_code AS company_cvm_code
            FROM alerts a
            JOIN companies c ON c.id = a.company_id
            WHERE a.id = %s
            """,
            (alert_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "NOT_FOUND",
                    "message": "Alerta nao encontrado",
                },
            )

    return {"data": dict(row)}  # type: ignore[arg-type]


@router.patch("/{alert_id}/read")
async def mark_alert_read(alert_id: int) -> dict[str, Any]:
    """Mark a single alert as read."""
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE alerts SET is_read = TRUE
            WHERE id = %s
            RETURNING id, is_read
            """,
            (alert_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "NOT_FOUND",
                    "message": "Alerta nao encontrado",
                },
            )

    return {"data": dict(row)}  # type: ignore[arg-type]


@router.patch("/mark-all-read")
async def mark_all_read(
    company_id: int | None = None,
    alert_type: str | None = None,
) -> dict[str, Any]:
    """Mark multiple alerts as read, optionally filtered by company or type."""
    with get_cursor() as cur:
        conditions = ["is_read = FALSE"]
        params: list[Any] = []

        if company_id is not None:
            conditions.append("company_id = %s")
            params.append(company_id)

        if alert_type:
            if alert_type not in _VALID_ALERT_TYPES:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "code": "INVALID_ALERT_TYPE",
                        "message": f"Tipo invalido. Valores aceitos: {', '.join(sorted(_VALID_ALERT_TYPES))}",
                    },
                )
            conditions.append("alert_type = %s")
            params.append(alert_type)

        where = "WHERE " + " AND ".join(conditions)
        cur.execute(
            f"UPDATE alerts SET is_read = TRUE {where} RETURNING id",
            params,
        )
        updated = cur.rowcount

    return {"data": {"updated": updated}}
