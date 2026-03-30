"""FastAPI application entry point."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.app.config import get_settings
from api.app.routers import (
    alerts,
    companies,
    correlations,
    dashboard,
    dividends,
    documents,
    financial_statements,
    holdings,
    insiders,
    material_facts,
    rankings,
    sync,
)

settings = get_settings()

app = FastAPI(
    title="InSight API",
    description="Inteligência em Movimentações de Insiders — CVM Brasil",
    version="0.1.0",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(alerts.router, prefix="/api")
app.include_router(companies.router, prefix="/api")
app.include_router(correlations.router, prefix="/api")
app.include_router(dividends.router, prefix="/api")
app.include_router(documents.router, prefix="/api")
app.include_router(financial_statements.router, prefix="/api")
app.include_router(holdings.router, prefix="/api")
app.include_router(insiders.router, prefix="/api")
app.include_router(material_facts.router, prefix="/api")
app.include_router(rankings.router, prefix="/api")
app.include_router(dashboard.router, prefix="/api")
app.include_router(sync.router, prefix="/api")


@app.get("/api/health")
async def health_check() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}
