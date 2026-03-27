"""Document endpoints."""

from fastapi import APIRouter

router = APIRouter(prefix="/documents", tags=["documents"])


@router.get("")
async def list_documents() -> dict:
    """List documents."""
    return {"data": [], "total": 0, "page": 1, "per_page": 20}
