"""Alert generator — detects atypical movements after sync.

Analyses recently-imported documents and their holdings to detect:
- volume_atipico:  abnormal trading volume vs 6-month average
- alto_valor:      high-value transactions by key insiders
- mudanca_direcao: direction reversal (all buys → sell or vice-versa)
- retorno_atividade: insider group resumes trading after 6-month silence

Usage (called from main.py after successful sync):
    from pipeline.src.alerts.alert_generator import generate_alerts
    n = generate_alerts(database_url, document_ids)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

HIGH_VALUE_MEDIUM = 500_000  # R$500K → medium
HIGH_VALUE_HIGH = 2_000_000  # R$2M  → high
VOLUME_MEDIUM_MULTIPLIER = 2.0
VOLUME_HIGH_MULTIPLIER = 3.0
HIGH_VALUE_GROUPS = ("Controlador", "Diretoria")


# ---------------------------------------------------------------------------
# Internal data structure
# ---------------------------------------------------------------------------


@dataclass
class _Alert:
    company_id: int
    holding_id: int | None
    alert_type: str
    severity: str
    title: str
    description: str
    metadata: dict[str, Any]


# ---------------------------------------------------------------------------
# Detection rules
# ---------------------------------------------------------------------------

# Note: cursor parameter typed as Any because we use DictCursor at runtime
# but psycopg2 stubs don't support string-key access on DictRow.


def _detect_alto_valor(
    cur: Any,
    document_ids: list[int],
) -> list[_Alert]:
    """Flag individual transactions > R$500K by Controlador/Diretoria."""
    cur.execute(
        """
        SELECT h.id            AS holding_id,
               h.total_value,
               h.operation_type,
               h.operation_date,
               h.insider_group,
               h.insider_name,
               d.company_id,
               d.reference_date,
               c.name          AS company_name,
               c.ticker
          FROM holdings h
          JOIN documents d ON h.document_id = d.id
          JOIN companies c ON d.company_id  = c.id
         WHERE h.document_id = ANY(%s)
           AND h.section      = 'movimentacoes'
           AND h.confidence  != 'baixa'
           AND h.total_value  > %s
           AND h.insider_group IN %s
        """,
        (document_ids, HIGH_VALUE_MEDIUM, HIGH_VALUE_GROUPS),
    )

    alerts: list[_Alert] = []
    for row in cur.fetchall():
        val = float(row["total_value"])
        severity = "high" if val > HIGH_VALUE_HIGH else "medium"
        alerts.append(
            _Alert(
                company_id=row["company_id"],
                holding_id=row["holding_id"],
                alert_type="alto_valor",
                severity=severity,
                title=f"Operação de alto valor: R${val:,.0f}",
                description=(
                    f"{row['insider_group']} — {row['insider_name'] or 'N/A'} "
                    f"realizou {row['operation_type'] or 'operação'} "
                    f"de R${val:,.2f} em {row['company_name']} "
                    f"({row['ticker'] or 'N/A'}) em {row['operation_date']}"
                ),
                metadata={
                    "reference_date": str(row["reference_date"]),
                    "total_value": val,
                    "operation_type": row["operation_type"],
                    "insider_group": row["insider_group"],
                    "insider_name": row["insider_name"],
                },
            )
        )
    return alerts


def _detect_volume_atipico(
    cur: Any,
    document_ids: list[int],
    company_refs: list[tuple[int, str]],
) -> list[_Alert]:
    """Compare current-month operation count to 6-month average per company."""
    alerts: list[_Alert] = []

    for company_id, ref_date in company_refs:
        # Current month operation count
        cur.execute(
            """
            SELECT COUNT(*) AS cnt
              FROM holdings h
              JOIN documents d ON h.document_id = d.id
             WHERE d.company_id = %s
               AND h.section    = 'movimentacoes'
               AND h.confidence != 'baixa'
               AND DATE_TRUNC('month', d.reference_date)
                   = DATE_TRUNC('month', %s::date)
            """,
            (company_id, ref_date),
        )
        result = cur.fetchone()
        current_count: int = result[0] if result else 0
        if current_count == 0:
            continue

        # Average monthly count over prior 6 months
        cur.execute(
            """
            SELECT COALESCE(AVG(monthly.cnt), 0) AS avg_cnt
              FROM (
                SELECT DATE_TRUNC('month', d.reference_date) AS m,
                       COUNT(*) AS cnt
                  FROM holdings h
                  JOIN documents d ON h.document_id = d.id
                 WHERE d.company_id = %s
                   AND h.section    = 'movimentacoes'
                   AND h.confidence != 'baixa'
                   AND d.reference_date >= (%s::date - INTERVAL '6 months')
                   AND DATE_TRUNC('month', d.reference_date)
                       < DATE_TRUNC('month', %s::date)
                 GROUP BY DATE_TRUNC('month', d.reference_date)
              ) monthly
            """,
            (company_id, ref_date, ref_date),
        )
        result = cur.fetchone()
        avg_count = float(result[0]) if result else 0.0
        if avg_count == 0:
            continue

        ratio = current_count / avg_count
        if ratio < VOLUME_MEDIUM_MULTIPLIER:
            continue

        severity = "high" if ratio >= VOLUME_HIGH_MULTIPLIER else "medium"

        # Fetch company info
        cur.execute(
            "SELECT name, ticker FROM companies WHERE id = %s",
            (company_id,),
        )
        comp = cur.fetchone()
        company_name: str = comp[0] if comp else "N/A"
        ticker: str | None = comp[1] if comp else None

        alerts.append(
            _Alert(
                company_id=company_id,
                holding_id=None,
                alert_type="volume_atipico",
                severity=severity,
                title=f"Volume atípico: {ratio:.1f}x a média",
                description=(
                    f"{company_name} ({ticker or 'N/A'}) teve "
                    f"{current_count} movimentações no mês, "
                    f"{ratio:.1f}x a média de {avg_count:.0f} "
                    f"nos últimos 6 meses"
                ),
                metadata={
                    "reference_date": str(ref_date),
                    "current_count": current_count,
                    "avg_count": avg_count,
                    "ratio": round(ratio, 2),
                },
            )
        )
    return alerts


def _detect_mudanca_direcao(
    cur: Any,
    document_ids: list[int],
    company_refs: list[tuple[int, str]],
) -> list[_Alert]:
    """Detect direction reversal: all buys then sell, or vice-versa."""
    alerts: list[_Alert] = []

    for company_id, ref_date in company_refs:
        # Historical operation types (prior 6 months)
        cur.execute(
            """
            SELECT DISTINCT h.operation_type
              FROM holdings h
              JOIN documents d ON h.document_id = d.id
             WHERE d.company_id = %s
               AND h.section    = 'movimentacoes'
               AND h.confidence != 'baixa'
               AND h.operation_type IS NOT NULL
               AND d.reference_date >= (%s::date - INTERVAL '6 months')
               AND DATE_TRUNC('month', d.reference_date)
                   < DATE_TRUNC('month', %s::date)
            """,
            (company_id, ref_date, ref_date),
        )
        hist_types = {row[0] for row in cur.fetchall()}
        if not hist_types:
            continue

        # New operation types from recently imported documents
        cur.execute(
            """
            SELECT DISTINCT h.operation_type
              FROM holdings h
             WHERE h.document_id = ANY(%s)
               AND h.section      = 'movimentacoes'
               AND h.confidence  != 'baixa'
               AND h.operation_type IS NOT NULL
               AND h.document_id IN (
                   SELECT id FROM documents WHERE company_id = %s
               )
            """,
            (document_ids, company_id),
        )
        new_types = {row[0] for row in cur.fetchall()}
        if not new_types:
            continue

        # Check for direction reversal
        buy_kw = {"Compra", "Aquisição"}
        sell_kw = {"Venda", "Alienação"}

        hist_buy = bool(hist_types & buy_kw) and not bool(hist_types & sell_kw)
        hist_sell = bool(hist_types & sell_kw) and not bool(hist_types & buy_kw)

        new_has_buy = bool(new_types & buy_kw)
        new_has_sell = bool(new_types & sell_kw)

        reversal = (hist_buy and new_has_sell) or (hist_sell and new_has_buy)
        if not reversal:
            continue

        cur.execute(
            "SELECT name, ticker FROM companies WHERE id = %s",
            (company_id,),
        )
        comp = cur.fetchone()
        company_name: str = comp[0] if comp else "N/A"
        ticker: str | None = comp[1] if comp else None

        old_dir = "compra" if hist_buy else "venda"
        new_dir = "venda" if hist_buy else "compra"

        alerts.append(
            _Alert(
                company_id=company_id,
                holding_id=None,
                alert_type="mudanca_direcao",
                severity="high",
                title=f"Mudança de direção: {old_dir} → {new_dir}",
                description=(
                    f"{company_name} ({ticker or 'N/A'}) teve apenas "
                    f"operações de {old_dir} nos últimos 6 meses, mas "
                    f"agora registrou operações de {new_dir}"
                ),
                metadata={
                    "reference_date": str(ref_date),
                    "historical_direction": old_dir,
                    "new_direction": new_dir,
                },
            )
        )
    return alerts


def _detect_retorno_atividade(
    cur: Any,
    document_ids: list[int],
    company_refs: list[tuple[int, str]],
) -> list[_Alert]:
    """Detect insider groups resuming activity after 6-month silence."""
    alerts: list[_Alert] = []

    # Get distinct (company_id, insider_group) from new documents
    cur.execute(
        """
        SELECT DISTINCT d.company_id, h.insider_group
          FROM holdings h
          JOIN documents d ON h.document_id = d.id
         WHERE h.document_id = ANY(%s)
           AND h.section      = 'movimentacoes'
           AND h.confidence  != 'baixa'
           AND h.insider_group IS NOT NULL
        """,
        (document_ids,),
    )
    new_pairs = cur.fetchall()

    # Build lookup for reference_date by company_id
    ref_by_company: dict[int, str] = {}
    for cid, rdate in company_refs:
        ref_by_company[cid] = str(rdate)

    for row in new_pairs:
        company_id: int = row[0]
        insider_group: str = row[1]
        ref_date = ref_by_company.get(company_id)
        if ref_date is None:
            continue

        # Check for prior activity in the 6 months before this ref_date
        cur.execute(
            """
            SELECT 1
              FROM holdings h
              JOIN documents d ON h.document_id = d.id
             WHERE d.company_id  = %s
               AND h.insider_group = %s
               AND h.section     = 'movimentacoes'
               AND h.confidence != 'baixa'
               AND d.reference_date >= (%s::date - INTERVAL '6 months')
               AND DATE_TRUNC('month', d.reference_date)
                   < DATE_TRUNC('month', %s::date)
             LIMIT 1
            """,
            (company_id, insider_group, ref_date, ref_date),
        )
        if cur.fetchone() is not None:
            # Had recent activity — not a return
            continue

        cur.execute(
            "SELECT name, ticker FROM companies WHERE id = %s",
            (company_id,),
        )
        comp = cur.fetchone()
        company_name: str = comp[0] if comp else "N/A"
        ticker: str | None = comp[1] if comp else None

        alerts.append(
            _Alert(
                company_id=company_id,
                holding_id=None,
                alert_type="retorno_atividade",
                severity="medium",
                title=f"Retorno de atividade: {insider_group}",
                description=(
                    f"{insider_group} de {company_name} ({ticker or 'N/A'}) "
                    f"voltou a movimentar após mais de 6 meses sem atividade"
                ),
                metadata={
                    "reference_date": str(ref_date),
                    "insider_group": insider_group,
                },
            )
        )
    return alerts


# ---------------------------------------------------------------------------
# Dedup + insert
# ---------------------------------------------------------------------------


def _insert_alerts(
    cur: Any,
    candidates: list[_Alert],
) -> int:
    """Insert alerts, skipping duplicates.

    Dedup key: company_id + alert_type + holding_id (for per-holding alerts)
    or company_id + alert_type + metadata.reference_date (for aggregate alerts).
    """
    if not candidates:
        return 0

    inserted = 0
    for a in candidates:
        ref_date = a.metadata.get("reference_date", "")

        # Dedup: per-holding for alto_valor, per company+type+ref_date otherwise
        if a.holding_id is not None:
            cur.execute(
                """
                SELECT 1 FROM alerts
                 WHERE company_id = %s
                   AND alert_type = %s
                   AND holding_id = %s
                 LIMIT 1
                """,
                (a.company_id, a.alert_type, a.holding_id),
            )
        else:
            cur.execute(
                """
                SELECT 1 FROM alerts
                 WHERE company_id = %s
                   AND alert_type = %s
                   AND metadata->>'reference_date' = %s
                 LIMIT 1
                """,
                (a.company_id, a.alert_type, ref_date),
            )

        if cur.fetchone() is not None:
            continue

        cur.execute(
            """
            INSERT INTO alerts
                (company_id, holding_id, alert_type, severity,
                 title, description, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                a.company_id,
                a.holding_id,
                a.alert_type,
                a.severity,
                a.title,
                a.description,
                json.dumps(a.metadata, ensure_ascii=False, default=str),
            ),
        )
        inserted += 1

    return inserted


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_alerts(database_url: str, document_ids: list[int]) -> int:
    """Generate alerts for recently imported documents.

    Returns the number of new alerts inserted.
    """
    if not document_ids:
        logger.info("No document IDs provided, skipping alert generation")
        return 0

    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Resolve company_id + reference_date from new documents
            cur.execute(
                """
                SELECT DISTINCT company_id, reference_date::text
                  FROM documents
                 WHERE id = ANY(%s)
                """,
                (document_ids,),
            )
            rows = cur.fetchall()
            company_refs: list[tuple[int, str]] = [
                (r[0], r[1]) for r in rows
            ]

            if not company_refs:
                logger.info("No companies found for given document IDs")
                return 0

            # Run detection rules
            candidates: list[_Alert] = []
            candidates.extend(_detect_alto_valor(cur, document_ids))
            candidates.extend(
                _detect_volume_atipico(cur, document_ids, company_refs)
            )
            candidates.extend(
                _detect_mudanca_direcao(cur, document_ids, company_refs)
            )
            candidates.extend(
                _detect_retorno_atividade(cur, document_ids, company_refs)
            )

            # Insert (with dedup)
            inserted = _insert_alerts(cur, candidates)

        conn.commit()

        companies_count = len({c.company_id for c in candidates})
        logger.info(
            "%d alerts generated for %d companies", inserted, companies_count
        )
        return inserted

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
