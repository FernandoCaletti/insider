"""Sync status endpoints."""

from typing import Any

from fastapi import APIRouter, Query

from api.app.database import get_cursor

router = APIRouter(prefix="/sync", tags=["sync"])


@router.get("/status")
async def sync_status() -> dict[str, Any]:
    """Get current sync status (latest sync log entry)."""
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id, started_at, finished_at, status,
                documents_found, documents_processed, documents_failed,
                error_details
            FROM sync_log
            ORDER BY started_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()

    if row is None:
        return {"data": None}

    return {"data": dict(row)}  # type: ignore[arg-type]


@router.get("/history")
async def sync_history(
    limit: int = Query(10, ge=1, le=100),
) -> dict[str, Any]:
    """Get sync history."""
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id, started_at, finished_at, status,
                documents_found, documents_processed, documents_failed,
                error_details
            FROM sync_log
            ORDER BY started_at DESC
            LIMIT %s
            """,
            [limit],
        )
        rows = cur.fetchall()

    return {"data": [dict(r) for r in rows]}  # type: ignore[arg-type]
