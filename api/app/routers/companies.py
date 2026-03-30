"""Company endpoints."""

import io
from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

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

        # Get current positions with initial vs final comparison from latest document
        cur.execute(
            """
            WITH latest_doc AS (
                SELECT id FROM documents
                WHERE company_id = %s
                ORDER BY reference_date DESC
                LIMIT 1
            ),
            total_shares AS (
                SELECT COALESCE(SUM(quantity), 0) AS total
                FROM insider_positions
                WHERE company_id = %s
                  AND reference_date = (
                      SELECT MAX(reference_date) FROM insider_positions WHERE company_id = %s
                  )
            )
            SELECT
                h.insider_group,
                h.asset_type,
                h.asset_description,
                SUM(CASE WHEN h.section = 'inicial' THEN h.quantity ELSE 0 END) AS qty_inicial,
                SUM(CASE WHEN h.section = 'final' THEN h.quantity ELSE 0 END) AS qty_final,
                SUM(CASE WHEN h.section = 'final' THEN h.quantity ELSE 0 END)
                  - SUM(CASE WHEN h.section = 'inicial' THEN h.quantity ELSE 0 END) AS variacao,
                CASE WHEN SUM(CASE WHEN h.section = 'inicial' THEN h.quantity ELSE 0 END) > 0
                     THEN ROUND(
                         (SUM(CASE WHEN h.section = 'final' THEN h.quantity ELSE 0 END)
                          - SUM(CASE WHEN h.section = 'inicial' THEN h.quantity ELSE 0 END))
                         / SUM(CASE WHEN h.section = 'inicial' THEN h.quantity ELSE 0 END) * 100, 2
                     )
                     ELSE NULL
                END AS variacao_pct,
                CASE WHEN (SELECT total FROM total_shares) > 0
                     THEN ROUND(
                         SUM(CASE WHEN h.section = 'final' THEN h.quantity ELSE 0 END)
                         / (SELECT total FROM total_shares) * 100, 4
                     )
                     ELSE NULL
                END AS pct_capital
            FROM holdings h
            JOIN latest_doc ld ON h.document_id = ld.id
            WHERE h.confidence != 'baixa'
              AND h.section IN ('inicial', 'final')
            GROUP BY h.insider_group, h.asset_type, h.asset_description
            HAVING SUM(CASE WHEN h.section = 'final' THEN h.quantity ELSE 0 END) > 0
            ORDER BY h.insider_group, h.asset_type
            """,
            (company_id, company_id, company_id),
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

        # Get paginated results with aliased columns for frontend
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT id, company_id, reference_date, year, month,
                   file_hash, file_name AS filename, original_url AS source_url,
                   page_count, is_scanned, processed_at, created_at
            FROM documents {where}
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
            if operation_type == "mercado":
                conditions.append("(h.operation_type ILIKE 'Compra%%' OR h.operation_type ILIKE 'Venda%%')")
            elif operation_type == "corporativa":
                conditions.append("NOT (h.operation_type ILIKE 'Compra%%' OR h.operation_type ILIKE 'Venda%%')")
            else:
                conditions.append("h.operation_type ILIKE %s")
                params.append(f"{operation_type}%")
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


@router.get("/{company_id}/material-facts")
async def get_company_material_facts(
    company_id: int,
    date_from: date | None = None,
    date_to: date | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get material facts for a specific company."""
    offset = (page - 1) * per_page

    with get_cursor() as cur:
        # Verify company exists
        cur.execute("SELECT id FROM companies WHERE id = %s", (company_id,))
        if not cur.fetchone():
            raise _not_found()

        conditions = ["company_id = %s"]
        params: list[Any] = [company_id]

        if date_from is not None:
            conditions.append("reference_date >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("reference_date <= %s")
            params.append(date_to)

        where = "WHERE " + " AND ".join(conditions)

        # Count
        cur.execute(
            f"SELECT COUNT(*) AS cnt FROM material_facts {where}",
            params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Paginated results
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT id, company_id, reference_date, category, subject,
                   source_url, cvm_code, protocol, delivery_date, created_at
            FROM material_facts {where}
            ORDER BY reference_date DESC, id DESC
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


@router.get("/{company_id}/alerts")
async def get_company_alerts(
    company_id: int,
    alert_type: str | None = None,
    severity: str | None = None,
    is_read: bool | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get alerts for a specific company."""
    offset = (page - 1) * per_page

    with get_cursor() as cur:
        # Verify company exists
        cur.execute("SELECT id FROM companies WHERE id = %s", (company_id,))
        if not cur.fetchone():
            raise _not_found()

        conditions = ["company_id = %s"]
        params: list[Any] = [company_id]

        if alert_type:
            conditions.append("alert_type = %s")
            params.append(alert_type)

        if severity:
            conditions.append("severity = %s")
            params.append(severity)

        if is_read is not None:
            conditions.append("is_read = %s")
            params.append(is_read)

        where = "WHERE " + " AND ".join(conditions)

        # Count
        cur.execute(
            f"SELECT COUNT(*) AS cnt FROM alerts {where}",
            params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Paginated results
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT id, company_id, holding_id, alert_type, severity,
                   title, description, metadata, is_read, created_at
            FROM alerts {where}
            ORDER BY created_at DESC, id DESC
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


@router.get("/{company_id}/financial-statements")
async def get_company_financial_statements(
    company_id: int,
    statement_type: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get financial statements for a specific company."""
    offset = (page - 1) * per_page

    with get_cursor() as cur:
        # Verify company exists
        cur.execute("SELECT id FROM companies WHERE id = %s", (company_id,))
        if not cur.fetchone():
            raise _not_found()

        conditions = ["company_id = %s"]
        params: list[Any] = [company_id]

        if statement_type:
            conditions.append("statement_type = %s")
            params.append(statement_type.upper())

        if date_from is not None:
            conditions.append("reference_date >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("reference_date <= %s")
            params.append(date_to)

        where = "WHERE " + " AND ".join(conditions)

        # Count
        cur.execute(
            f"SELECT COUNT(*) AS cnt FROM financial_statements {where}",
            params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Paginated results
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT id, company_id, reference_date, statement_type,
                   account_code, account_name, value, currency,
                   source_url, created_at
            FROM financial_statements {where}
            ORDER BY reference_date DESC, statement_type, account_code
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


@router.get("/{company_id}/dividends")
async def get_company_dividends(
    company_id: int,
    dividend_type: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Get dividends for a specific company."""
    offset = (page - 1) * per_page

    with get_cursor() as cur:
        # Verify company exists
        cur.execute("SELECT id FROM companies WHERE id = %s", (company_id,))
        if not cur.fetchone():
            raise _not_found()

        conditions = ["company_id = %s"]
        params: list[Any] = [company_id]

        if dividend_type:
            conditions.append("dividend_type ILIKE %s")
            params.append(f"%{dividend_type}%")

        if date_from is not None:
            conditions.append("ex_date >= %s")
            params.append(date_from)

        if date_to is not None:
            conditions.append("ex_date <= %s")
            params.append(date_to)

        where = "WHERE " + " AND ".join(conditions)

        # Count
        cur.execute(
            f"SELECT COUNT(*) AS cnt FROM dividends {where}",
            params,
        )
        total = cur.fetchone()["cnt"]  # type: ignore[index]

        # Paginated results
        params.extend([per_page, offset])
        cur.execute(
            f"""
            SELECT id, company_id, ex_date, payment_date, record_date,
                   dividend_type, value_per_share, total_value,
                   currency, source_url, created_at
            FROM dividends {where}
            ORDER BY ex_date DESC, id DESC
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


@router.get("/{company_id}/report")
async def generate_company_report(company_id: int) -> StreamingResponse:
    """Generate a PDF report for a company with insider movements summary."""
    from reportlab.lib import colors  # type: ignore[import-untyped]
    from reportlab.lib.pagesizes import A4  # type: ignore[import-untyped]
    from reportlab.lib.styles import getSampleStyleSheet  # type: ignore[import-untyped]
    from reportlab.lib.units import mm  # type: ignore[import-untyped]
    from reportlab.platypus import SimpleDocTemplate, Table as RLTable, TableStyle, Paragraph, Spacer  # type: ignore[import-untyped]

    with get_cursor() as cur:
        # Company info
        cur.execute("SELECT * FROM companies WHERE id = %s", (company_id,))
        company = cur.fetchone()
        if not company:
            raise _not_found()
        company_dict: dict[str, Any] = dict(company)  # type: ignore[arg-type]

        # Movement summary stats
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_movements,
                COUNT(*) FILTER (WHERE h.operation_type IN ('Compra', 'Aquisição')) AS buys,
                COUNT(*) FILTER (WHERE h.operation_type IN ('Venda', 'Alienação')) AS sells,
                COALESCE(SUM(ABS(h.total_value)) FILTER (WHERE h.operation_type IN ('Compra', 'Aquisição')), 0) AS buy_value,
                COALESCE(SUM(ABS(h.total_value)) FILTER (WHERE h.operation_type IN ('Venda', 'Alienação')), 0) AS sell_value,
                MIN(h.operation_date) AS first_movement,
                MAX(h.operation_date) AS last_movement
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            WHERE d.company_id = %s AND h.section = 'movimentacoes' AND h.confidence != 'baixa'
            """,
            (company_id,),
        )
        stats_row = cur.fetchone()
        stats: dict[str, Any] = dict(stats_row) if stats_row else {}  # type: ignore[arg-type]

        # Top insiders by total value
        cur.execute(
            """
            SELECT h.insider_name, COUNT(*) AS operations,
                   COALESCE(SUM(ABS(h.total_value)), 0) AS total_value
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            WHERE d.company_id = %s AND h.section = 'movimentacoes'
              AND h.confidence != 'baixa' AND h.insider_name IS NOT NULL
            GROUP BY h.insider_name
            ORDER BY total_value DESC
            LIMIT 10
            """,
            (company_id,),
        )
        top_insiders = [dict(r) for r in cur.fetchall()]  # type: ignore[arg-type]

        # Recent movements
        cur.execute(
            """
            SELECT h.operation_date, h.asset_type, h.operation_type,
                   h.quantity, h.total_value, h.insider_name, h.insider_group
            FROM holdings h
            JOIN documents d ON d.id = h.document_id
            WHERE d.company_id = %s AND h.section = 'movimentacoes'
              AND h.confidence != 'baixa'
            ORDER BY h.operation_date DESC NULLS LAST, h.id DESC
            LIMIT 20
            """,
            (company_id,),
        )
        recent = [dict(r) for r in cur.fetchall()]  # type: ignore[arg-type]

        # Alert count
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM alerts WHERE company_id = %s",
            (company_id,),
        )
        alert_count = cur.fetchone()["cnt"]  # type: ignore[index]

    # Build PDF
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=15 * mm, bottomMargin=15 * mm,
    )
    styles = getSampleStyleSheet()
    elements: list[Any] = []

    # Title
    elements.append(Paragraph(f"Relatório — {company_dict.get('name', '')}", styles["Title"]))
    elements.append(Spacer(1, 4 * mm))

    # Company info
    ticker = company_dict.get("ticker") or "—"
    cnpj = company_dict.get("cnpj") or "—"
    sector = company_dict.get("sector") or "—"
    elements.append(Paragraph(
        f"<b>Ticker:</b> {ticker} &nbsp;&nbsp; <b>CNPJ:</b> {cnpj} &nbsp;&nbsp; <b>Setor:</b> {sector}",
        styles["Normal"],
    ))
    elements.append(Spacer(1, 6 * mm))

    # Summary section
    elements.append(Paragraph("Resumo de Movimentações", styles["Heading2"]))

    def _fmt_currency(val: Any) -> str:
        if val is None:
            return "—"
        return f"R$ {float(val):,.2f}"

    summary_data = [
        ["Total de movimentações", str(stats.get("total_movements", 0))],
        ["Compras", str(stats.get("buys", 0))],
        ["Vendas", str(stats.get("sells", 0))],
        ["Valor de compras", _fmt_currency(stats.get("buy_value"))],
        ["Valor de vendas", _fmt_currency(stats.get("sell_value"))],
        ["Primeira movimentação", str(stats.get("first_movement") or "—")],
        ["Última movimentação", str(stats.get("last_movement") or "—")],
        ["Alertas", str(alert_count)],
    ]
    t = RLTable(summary_data, colWidths=[120 * mm, 50 * mm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.Color(0.94, 0.94, 0.94)),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.Color(0.8, 0.8, 0.8)),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 8 * mm))

    # Top insiders
    if top_insiders:
        elements.append(Paragraph("Principais Insiders (por valor)", styles["Heading2"]))
        insider_table_data: list[list[str]] = [["Insider", "Operações", "Valor Total"]]
        for ins in top_insiders:
            insider_table_data.append([
                str(ins.get("insider_name") or "—"),
                str(ins.get("operations", 0)),
                _fmt_currency(ins.get("total_value")),
            ])
        t2 = RLTable(insider_table_data, colWidths=[90 * mm, 30 * mm, 50 * mm])
        t2.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.Color(0.12, 0.31, 0.47)),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.Color(0.8, 0.8, 0.8)),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.Color(0.96, 0.96, 0.96)]),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        elements.append(t2)
        elements.append(Spacer(1, 8 * mm))

    # Recent movements table
    if recent:
        elements.append(Paragraph("Movimentações Recentes", styles["Heading2"]))
        mov_data: list[list[str]] = [["Data", "Ativo", "Operação", "Qtd", "Valor", "Insider", "Grupo"]]
        for m in recent:
            mov_data.append([
                str(m.get("operation_date") or "—"),
                str(m.get("asset_type") or "—"),
                str(m.get("operation_type") or "—"),
                f"{float(m['quantity']):,.0f}" if m.get("quantity") else "—",
                _fmt_currency(m.get("total_value")),
                str(m.get("insider_name") or "—"),
                str(m.get("insider_group") or "—"),
            ])
        col_w = [22 * mm, 22 * mm, 20 * mm, 22 * mm, 25 * mm, 40 * mm, 28 * mm]
        t3 = RLTable(mov_data, colWidths=col_w)
        t3.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.Color(0.12, 0.31, 0.47)),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 7),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.Color(0.8, 0.8, 0.8)),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.Color(0.96, 0.96, 0.96)]),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        elements.append(t3)

    doc.build(elements)
    buf.seek(0)

    safe_name = (company_dict.get("ticker") or company_dict.get("name") or "company").replace(" ", "_")
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=relatorio_{safe_name}.pdf"},
    )
