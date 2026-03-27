"""PDF extractor for CVM insider trading forms using pdfplumber.

Extracts structured holdings data from CVM 'Valores Mobiliários Negociados
e Detidos' PDF filings.  Each PDF may contain multiple consolidated forms
(one per insider).  The parser splits them, detects sections (saldo inicial,
movimentações, saldo final), normalises numbers, maps asset types and assigns
a confidence level to every record.

Company identification always comes from the metadata CSV (cvm_code), never
from text inside the PDF.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Literal

import pdfplumber  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

AssetType = Literal[
    "ACAO_ON", "ACAO_PN", "DEBENTURE", "OPCAO_COMPRA", "OPCAO_VENDA", "OPCAO", "BDR", "UNIT", "OUTRO"
]
Section = Literal["inicial", "movimentacoes", "final"]
Confidence = Literal["alta", "media", "baixa"]

# ---------------------------------------------------------------------------
# Asset-type regex mapping (order matters – more specific patterns first)
# ---------------------------------------------------------------------------

ASSET_TYPE_PATTERNS: list[tuple[re.Pattern[str], AssetType]] = [
    (re.compile(r"\bordin[aá]ri[ao]s?\b|\bON\b"), "ACAO_ON"),
    (re.compile(r"\bpreferen[cs]i[aá][il]s?\b|\bPN[A-Z]?\b"), "ACAO_PN"),
    (re.compile(r"\bdeb[eê]nture", re.IGNORECASE), "DEBENTURE"),
    (re.compile(r"\bop[çc][ãa]o\s+de\s+compra|\bcall\b", re.IGNORECASE), "OPCAO_COMPRA"),
    (re.compile(r"\bop[çc][ãa]o\s+de\s+venda|\bput\b", re.IGNORECASE), "OPCAO_VENDA"),
    (re.compile(r"\bop[çc][ãa]o|\boptions?\b", re.IGNORECASE), "OPCAO"),
    (re.compile(r"\bBDR\b", re.IGNORECASE), "BDR"),
    (re.compile(r"\bUNIT\b", re.IGNORECASE), "UNIT"),
    # Generic "Ações" without ON/PN qualifier — default to ON
    (re.compile(r"\b[Aa][çc][õo]es\b"), "ACAO_ON"),
]

# ---------------------------------------------------------------------------
# Section-detection regexes
# ---------------------------------------------------------------------------

_SECTION_PATTERNS: list[tuple[re.Pattern[str], Section]] = [
    (re.compile(r"saldo\s+inicial|posi[çc][ãa]o\s+inicial", re.IGNORECASE), "inicial"),
    (
        re.compile(
            r"movimenta[çc][õo]es|opera[çc][õo]es\s+realizadas|transa[çc][õo]es",
            re.IGNORECASE,
        ),
        "movimentacoes",
    ),
    (re.compile(r"saldo\s+final|posi[çc][ãa]o\s+final", re.IGNORECASE), "final"),
]

# ---------------------------------------------------------------------------
# No-operations detection patterns
# ---------------------------------------------------------------------------

# Primary pattern: "(X) não foram realizadas operações" — the checkbox is marked.
_NO_OPS_CHECKED = re.compile(
    r"\(\s*[Xx]\s*\)\s*n[ãa]o\s+foram\s+realizadas",
    re.IGNORECASE,
)
# Fallback patterns for forms that don't use the checkbox convention.
_NO_OPS_FALLBACK: list[re.Pattern[str]] = [
    re.compile(r"n[ãa]o\s+houve\s+movimenta[çc][ãa]o", re.IGNORECASE),
    re.compile(r"sem\s+movimenta[çc][ãa]o", re.IGNORECASE),
]

# ---------------------------------------------------------------------------
# Insider group detection
# ---------------------------------------------------------------------------

_INSIDER_GROUPS = [
    ("Controlador", re.compile(r"\(\s*[Xx]\s*\)\s*Controlador", re.IGNORECASE)),
    ("Conselho de Administração", re.compile(r"\(\s*[Xx]\s*\)\s*Conselho\s+Administra", re.IGNORECASE)),
    ("Diretoria", re.compile(r"\(\s*[Xx]\s*\)\s*Diret", re.IGNORECASE)),
    ("Conselho Fiscal", re.compile(r"\(\s*[Xx]\s*\)\s*Conselho\s+Fiscal", re.IGNORECASE)),
    ("Órgãos Técnicos", re.compile(r"\(\s*[Xx]\s*\)\s*[ÓO]rg[ãa]os\s+T[ée]cnicos", re.IGNORECASE)),
    ("Pessoas Ligadas", re.compile(r"\(\s*[Xx]\s*\)\s*Pessoas\s+Ligadas", re.IGNORECASE)),
]


def detect_insider_group(form_text: str, tables: list[list[list[str | None]]] | None = None) -> str | None:
    """Detect which insider group checkbox is marked with (X).

    Searches both the extracted text and raw table cell contents,
    since pdfplumber may split the group label across cells/lines.
    """
    # Search in form text
    for group_name, pattern in _INSIDER_GROUPS:
        if pattern.search(form_text):
            return group_name
    # Search in table cells (the (X) may be in a cell with line breaks)
    if tables:
        for table in tables:
            for row in table:
                if not row:
                    continue
                row_text = " ".join(str(c or "") for c in row)
                for group_name, pattern in _INSIDER_GROUPS:
                    if pattern.search(row_text):
                        return group_name
    return None


# ---------------------------------------------------------------------------
# Insider name detection
# ---------------------------------------------------------------------------

# Patterns for the insider name label in form text.
# CVM forms typically have "Nome:" or "Informante:" followed by the name.
_INSIDER_NAME_PATTERNS: list[re.Pattern[str]] = [
    # "Nome: FULANO DE TAL" or "Nome do Informante: FULANO"
    re.compile(
        r"(?:Nome(?:\s+do\s+(?:Informante|Declarante))?\s*[:\-]\s*)([A-ZÁÀÂÃÉÈÊÍÏÓÔÕÖÚÇÑ][A-ZÁÀÂÃÉÈÊÍÏÓÔÕÖÚÇÑa-záàâãéèêíïóôõöúçñ\s.'-]{2,80})",
        re.MULTILINE,
    ),
    # "Informante: FULANO DE TAL"
    re.compile(
        r"Informante\s*[:\-]\s*([A-ZÁÀÂÃÉÈÊÍÏÓÔÕÖÚÇÑ][A-ZÁÀÂÃÉÈÊÍÏÓÔÕÖÚÇÑa-záàâãéèêíïóôõöúçñ\s.'-]{2,80})",
        re.MULTILINE,
    ),
]


def detect_insider_name(form_text: str, tables: list[list[list[str | None]]] | None = None) -> str | None:
    """Extract the insider's name from form text or table cells.

    Searches for labels like 'Nome:', 'Informante:', etc. followed by
    a person's name.  Returns the cleaned name or ``None``.
    """
    # Search in form text
    for pattern in _INSIDER_NAME_PATTERNS:
        m = pattern.search(form_text)
        if m:
            name = m.group(1).strip().rstrip(".")
            # Skip obviously wrong matches (too short or looks like a section header)
            if len(name) >= 3 and not _is_section_label(name):
                return name

    # Search in table cells
    if tables:
        for table in tables:
            for row in table:
                if not row:
                    continue
                for i, cell in enumerate(row):
                    cell_text = str(cell or "").strip()
                    cell_lower = cell_text.lower()
                    if cell_lower in ("nome", "informante", "nome do informante", "nome do declarante"):
                        # Name should be in the next cell
                        if i + 1 < len(row):
                            name = str(row[i + 1] or "").strip()
                            if len(name) >= 3 and not _is_section_label(name):
                                return name

    return None


def _is_section_label(text: str) -> bool:
    """Return True if *text* looks like a section header, not a name."""
    lower = text.lower()
    return any(
        kw in lower
        for kw in ("saldo", "movimenta", "inicial", "final", "formulário", "consolidado", "valores")
    )


# ---------------------------------------------------------------------------
# Transaction day extraction
# ---------------------------------------------------------------------------


def extract_transaction_day(operation_date: str | None, raw_date: str | None = None) -> int | None:
    """Extract the day-of-month from a parsed operation date or raw date string.

    Args:
        operation_date: Parsed date in YYYY-MM-DD format.
        raw_date: Raw date string from PDF (e.g. "29", "15/03/2025").

    Returns:
        Day of month (1-31) or None.
    """
    # Try from parsed operation_date (YYYY-MM-DD)
    if operation_date and len(operation_date) >= 10:
        try:
            return int(operation_date[8:10])
        except (ValueError, IndexError):
            pass

    # Try from raw date string
    if raw_date:
        raw = raw_date.strip()
        # Bare day number (from "Dia" column)
        if re.match(r"^\d{1,2}$", raw):
            day = int(raw)
            if 1 <= day <= 31:
                return day
        # DD/MM/YYYY
        m = re.match(r"(\d{2})/\d{2}/\d{4}", raw)
        if m:
            return int(m.group(1))

    return None


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class HoldingRecord:
    """A single holding or movement extracted from a CVM form."""

    section: Section
    asset_type: AssetType
    asset_description: str
    quantity: Decimal | None = None
    unit_price: Decimal | None = None
    total_value: Decimal | None = None
    operation_type: str | None = None
    operation_date: str | None = None  # YYYY-MM-DD
    broker: str | None = None
    confidence: Confidence = "alta"
    insider_group: str | None = None
    insider_name: str | None = None
    transaction_day: int | None = None


@dataclass
class FormResult:
    """Parsed result from one consolidated form within a PDF."""

    holdings: list[HoldingRecord] = field(default_factory=list)
    has_operations: bool = True
    balance_validated: bool = False
    validation_notes: list[str] = field(default_factory=list)


@dataclass
class ExtractionResult:
    """Full result from parsing a CVM PDF file."""

    forms: list[FormResult] = field(default_factory=list)
    page_count: int = 0
    is_scanned: bool = False
    errors: list[str] = field(default_factory=list)

    @property
    def all_holdings(self) -> list[HoldingRecord]:
        """Flatten holdings from every form."""
        result: list[HoldingRecord] = []
        for form in self.forms:
            result.extend(form.holdings)
        return result


# ---------------------------------------------------------------------------
# Number / date helpers
# ---------------------------------------------------------------------------


def normalize_number(raw: str) -> Decimal | None:
    """Parse a Brazilian-formatted number into :class:`Decimal`.

    Removes thousands separators (``'.'``), replaces the decimal comma with a
    period, and strips stray whitespace / non-numeric characters.

    Examples::

        "1.925.000,00" → Decimal("1925000.00")
        "12,50"        → Decimal("12.50")
    """
    if not raw or not raw.strip():
        return None
    text = raw.strip()
    text = re.sub(r"[^\d.,-]", "", text)
    if not text or text in ("", "-", "."):
        return None
    # Brazilian: dots are thousands seps, comma is decimal
    text = text.replace(".", "").replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation:
        logger.debug("Could not parse number: %r", raw)
        return None


def normalize_price(raw: str, max_decimals: int = 8) -> Decimal | None:
    """Like :func:`normalize_number` but rounds to *max_decimals* places."""
    value = normalize_number(raw)
    if value is not None:
        quantize_str = "0." + "0" * max_decimals
        value = value.quantize(Decimal(quantize_str))
    return value


def parse_date(raw: str, ref_month: int = 0, ref_year: int = 0) -> str | None:
    """Parse a date string into ``YYYY-MM-DD``.

    Supports ``DD/MM/YYYY``, ``YYYY-MM-DD``, and bare day numbers (``29``)
    when *ref_month* and *ref_year* are supplied from the form header.
    """
    if not raw or not raw.strip():
        return None
    text = raw.strip()
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", text)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    if re.match(r"\d{4}-\d{2}-\d{2}$", text):
        return text
    # Bare day number (e.g. "29" from the "Dia" column)
    if re.match(r"\d{1,2}$", text) and ref_month and ref_year:
        day = int(text)
        if 1 <= day <= 31:
            return f"{ref_year}-{ref_month:02d}-{day:02d}"
    return None


# Regex to extract month/year from form header like "Em 01/2025" or "Em 12/2024"
_FORM_PERIOD_RE = re.compile(r"Em\s+(\d{1,2})/(\d{4})", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------


def classify_asset_type(description: str) -> AssetType:
    """Map a free-text asset description to an :data:`AssetType` enum."""
    for pattern, asset_type in ASSET_TYPE_PATTERNS:
        if pattern.search(description):
            return asset_type
    return "OUTRO"


def assign_confidence(record: HoldingRecord) -> Confidence:
    """Determine confidence based on field completeness."""
    if record.section == "movimentacoes":
        if (
            record.quantity is not None
            and record.unit_price is not None
            and record.total_value is not None
            and record.operation_type
            and record.operation_date
            and record.broker
        ):
            return "alta"
        if record.quantity is not None and record.asset_description:
            return "media"
        return "baixa"

    # inicial / final
    if record.quantity is not None and record.total_value is not None:
        return "alta"
    if record.quantity is not None or record.total_value is not None:
        return "media"
    return "baixa"


# ---------------------------------------------------------------------------
# PDF-level helpers
# ---------------------------------------------------------------------------


def is_scanned_pdf(pdf: pdfplumber.PDF) -> bool:  # type: ignore[name-defined]
    """Return ``True`` if the PDF appears to be a scanned image."""
    for page in pdf.pages:
        text: str = page.extract_text() or ""
        if len(text.strip()) > 50:
            return False
    return True


# ---------------------------------------------------------------------------
# Text splitting / section detection
# ---------------------------------------------------------------------------


def split_forms(text: str) -> list[str]:
    """Split full-PDF text into individual forms by *FORMULÁRIO CONSOLIDADO*."""
    pattern = re.compile(r"FORMUL[AÁ]RIO\s+CONSOLIDADO", re.IGNORECASE)
    parts = pattern.split(text)
    forms: list[str] = []
    for i, part in enumerate(parts):
        stripped = part.strip()
        if i == 0 and len(stripped) < 100:
            continue  # header noise before first marker
        if stripped:
            forms.append(stripped)
    if not forms and text.strip():
        forms = [text.strip()]
    return forms


def detect_section(line: str) -> Section | None:
    """Return the section a header line belongs to, or ``None``."""
    for pattern, section in _SECTION_PATTERNS:
        if pattern.search(line):
            return section
    return None


def detect_no_operations(form_text: str) -> bool:
    """Detect the 'não foram realizadas operações' checkbox.

    Only returns True when the (X) checkbox is specifically on the
    'não foram realizadas' line, not merely when that phrase appears
    in the form (both checked and unchecked options contain the text).
    """
    if _NO_OPS_CHECKED.search(form_text):
        return True
    for pat in _NO_OPS_FALLBACK:
        if pat.search(form_text):
            return True
    return False


def concatenate_broker_lines(broker: str | None) -> str | None:
    """Join broker names that break across two lines."""
    if not broker:
        return None
    result = re.sub(r"\s*\n\s*", " ", broker).strip()
    return result if result else None


# ---------------------------------------------------------------------------
# Column identification
# ---------------------------------------------------------------------------

_HEADER_KEYWORDS: dict[str, list[str]] = {
    "asset": ["tipo", "ativo", "título", "titulo", "valor mobili", "descrição", "descricao"],
    "quantity": ["quantidade", "qtd", "qtde"],
    "price": ["preço", "preco", "unitário", "unitario", "preço unit", "preco unit"],
    "value": ["valor total", "volume"],
    "operation": ["operação", "operacao", "tipo oper", "natureza"],
    "date": ["data", "dia"],
    "broker": ["corretora", "intermediário", "intermediario", "instituição", "instituicao"],
}


def _identify_columns(headers: list[str]) -> dict[str, int]:
    """Map semantic column names to indices from header text."""
    col_map: dict[str, int] = {}
    for i, h in enumerate(headers):
        h_lower = h.lower()
        for col_name, keywords in _HEADER_KEYWORDS.items():
            if any(kw in h_lower for kw in keywords):
                col_map.setdefault(col_name, i)
    return col_map


def _get_cell(cells: list[str], index: int | None) -> str:
    """Safely get a cell value by index."""
    if index is None or index >= len(cells):
        return ""
    return cells[index].strip()


# ---------------------------------------------------------------------------
# Table extraction
# ---------------------------------------------------------------------------


def _parse_row_to_record(
    cells: list[str],
    col_map: dict[str, int],
    section: Section,
    ref_month: int = 0,
    ref_year: int = 0,
) -> HoldingRecord | None:
    """Convert a single table row into a :class:`HoldingRecord`."""
    if len(cells) < 2:
        return None

    asset_idx = col_map.get("asset", 0)
    asset_desc = _get_cell(cells, asset_idx)
    if not asset_desc or len(asset_desc) < 2:
        return None

    # Include adjacent cell (often "Características dos Títulos") for
    # better asset type classification (e.g. "Ações" + "ON" → ACAO_ON)
    next_cell = _get_cell(cells, asset_idx + 1) if asset_idx + 1 < len(cells) else ""
    classify_text = f"{asset_desc} {next_cell}".strip()

    record = HoldingRecord(
        section=section,
        asset_type=classify_asset_type(classify_text),
        asset_description=classify_text[:500],
        quantity=normalize_number(_get_cell(cells, col_map.get("quantity"))),
        unit_price=normalize_price(_get_cell(cells, col_map.get("price"))),
        total_value=normalize_number(_get_cell(cells, col_map.get("value"))),
    )

    if section == "movimentacoes":
        record.operation_type = _get_cell(cells, col_map.get("operation")) or None
        raw_date = _get_cell(cells, col_map.get("date"))
        record.operation_date = parse_date(raw_date, ref_month, ref_year) if raw_date else None
        record.transaction_day = extract_transaction_day(record.operation_date, raw_date or None)
        record.broker = concatenate_broker_lines(
            _get_cell(cells, col_map.get("broker")) or None
        )

    record.confidence = assign_confidence(record)
    return record


def _parse_tables(
    tables: list[list[list[str | None]]],
    section: Section,
    ref_month: int = 0,
    ref_year: int = 0,
) -> list[HoldingRecord]:
    """Parse pdfplumber tables into :class:`HoldingRecord` list.

    Section headers (Saldo Inicial, Movimentações, Saldo Final) may appear
    as rows within a single large table.  We detect them row-by-row and
    switch section context + column mapping accordingly.
    """
    records: list[HoldingRecord] = []
    current_section = section
    col_map: dict[str, int] = {}

    for table in tables:
        if not table or len(table) < 2:
            continue
        for row in table:
            if not row:
                continue
            cells = [str(cell or "").strip() for cell in row]
            row_text = " ".join(cells)

            # Try to extract form period (e.g. "Em 01/2025")
            period_match = _FORM_PERIOD_RE.search(row_text)
            if period_match:
                ref_month = int(period_match.group(1))
                ref_year = int(period_match.group(2))

            # Check if this row is a section header
            detected = detect_section(row_text)
            if detected:
                current_section = detected
                col_map = {}
                continue

            # Check if this row is a column header row
            row_lower = [c.lower() for c in cells]
            candidate_map = _identify_columns(row_lower)
            if candidate_map and len(candidate_map) >= 2:
                col_map = candidate_map
                continue

            # Skip rows that are all empty or have very little content
            if all(not c for c in cells):
                continue

            rec = _parse_row_to_record(cells, col_map, current_section, ref_month, ref_year)
            if rec:
                records.append(rec)
    return records


# ---------------------------------------------------------------------------
# Text-based fallback extraction
# ---------------------------------------------------------------------------

# Pattern: description followed by 2-4 number groups (qty, price, value)
_TEXT_ROW_RE = re.compile(
    r"^(.{3,80}?)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s*$"
)

# Movement row with operation type and date
_MOVE_ROW_RE = re.compile(
    r"^(.{3,80}?)\s+"        # asset description
    r"(Compra|Venda|C|V)\s+"  # operation type
    r"(\d{2}/\d{2}/\d{4})\s+" # date
    r"([\d.,]+)\s+"           # quantity
    r"([\d.,]+)\s+"           # price
    r"([\d.,]+)"              # value
    r"(?:\s+(.+))?\s*$",      # optional broker
    re.IGNORECASE,
)


def _extract_from_text(
    lines: list[str],
) -> list[HoldingRecord]:
    """Fallback: extract holdings from raw text lines using regex."""
    records: list[HoldingRecord] = []
    current_section: Section = "inicial"
    prev_broker_record: HoldingRecord | None = None

    for line in lines:
        detected = detect_section(line)
        if detected:
            current_section = detected
            prev_broker_record = None
            continue

        # Try movement-specific pattern first
        if current_section == "movimentacoes":
            m = _MOVE_ROW_RE.match(line.strip())
            if m:
                desc = m.group(1).strip()
                raw_date_str = m.group(3)
                parsed_date = parse_date(raw_date_str)
                rec = HoldingRecord(
                    section="movimentacoes",
                    asset_type=classify_asset_type(desc),
                    asset_description=desc[:500],
                    operation_type=m.group(2).strip(),
                    operation_date=parsed_date,
                    transaction_day=extract_transaction_day(parsed_date, raw_date_str),
                    quantity=normalize_number(m.group(4)),
                    unit_price=normalize_price(m.group(5)),
                    total_value=normalize_number(m.group(6)),
                    broker=concatenate_broker_lines(m.group(7)),
                )
                rec.confidence = assign_confidence(rec)
                records.append(rec)
                prev_broker_record = rec
                continue

        # Generic pattern (desc + 3 numbers)
        m = _TEXT_ROW_RE.match(line.strip())
        if m:
            desc = m.group(1).strip()
            if len(desc) < 2:
                continue
            rec = HoldingRecord(
                section=current_section,
                asset_type=classify_asset_type(desc),
                asset_description=desc[:500],
                quantity=normalize_number(m.group(2)),
                unit_price=normalize_price(m.group(3)),
                total_value=normalize_number(m.group(4)),
            )
            rec.confidence = assign_confidence(rec)
            records.append(rec)
            prev_broker_record = rec
            continue

        # Broker continuation line: short text after a record with no broker
        stripped = line.strip()
        if (
            prev_broker_record
            and prev_broker_record.section == "movimentacoes"
            and not prev_broker_record.broker
            and stripped
            and len(stripped) < 120
            and not re.match(r"^[\d.,]+$", stripped)
            and not detect_section(stripped)
        ):
            prev_broker_record.broker = stripped
            prev_broker_record.confidence = assign_confidence(prev_broker_record)
            prev_broker_record = None
            continue

        prev_broker_record = None

    return records


# ---------------------------------------------------------------------------
# Balance validation
# ---------------------------------------------------------------------------


def _validate_balances(result: FormResult) -> None:
    """Check that *saldo_inicial + movimentações ≈ saldo_final* per asset."""
    by_asset: dict[str, dict[str, Decimal]] = {}

    for h in result.holdings:
        key = h.asset_description
        if key not in by_asset:
            by_asset[key] = {}
        qty = h.quantity or Decimal("0")

        if h.section in ("inicial", "final"):
            by_asset[key][h.section] = qty
        elif h.section == "movimentacoes":
            current = by_asset[key].get("movimentacoes", Decimal("0"))
            op = (h.operation_type or "").lower()
            if any(w in op for w in ("vend", "sell", "alien", "v")):
                current -= qty
            else:
                current += qty
            by_asset[key]["movimentacoes"] = current

    validated = True
    for asset, sections in by_asset.items():
        inicial = sections.get("inicial", Decimal("0"))
        moves = sections.get("movimentacoes", Decimal("0"))
        final = sections.get("final")

        if final is not None:
            expected = inicial + moves
            diff = abs(expected - final)
            tolerance = max(abs(final) * Decimal("0.01"), Decimal("1"))
            if diff > tolerance:
                note = (
                    f"Balance mismatch for '{asset}': "
                    f"inicial({inicial}) + moves({moves}) = {expected}, "
                    f"but final = {final} (diff={diff})"
                )
                logger.warning(note)
                result.validation_notes.append(note)
                validated = False

    result.balance_validated = validated


# ---------------------------------------------------------------------------
# Form parsing
# ---------------------------------------------------------------------------


def _parse_form(
    form_text: str,
    tables: list[list[list[str | None]]],
) -> FormResult:
    """Parse a single consolidated form (text + tables) into a :class:`FormResult`."""
    result = FormResult()

    if detect_no_operations(form_text):
        result.has_operations = False

    # Detect insider group (Controlador, Conselho, Diretoria, etc.)
    insider_group = detect_insider_group(form_text, tables)

    # Detect insider name
    insider_name = detect_insider_name(form_text, tables)

    # Determine section markers in the text
    lines = form_text.split("\n")

    # Try table-based extraction: _parse_tables detects section headers
    # (Saldo Inicial, Movimentações, Saldo Final) row-by-row within tables.
    if tables:
        result.holdings.extend(_parse_tables(tables, "inicial"))

    # If table extraction yielded nothing, fall back to text
    if not result.holdings:
        result.holdings = _extract_from_text(lines)

    # Tag all holdings with the insider group and name
    for h in result.holdings:
        h.insider_group = insider_group
        h.insider_name = insider_name

    # For no-operations forms keep only inicial/final
    if not result.has_operations:
        result.holdings = [
            h for h in result.holdings if h.section in ("inicial", "final")
        ]

    _validate_balances(result)
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_pdf(file_path: str) -> ExtractionResult:
    """Extract structured data from a CVM insider-trading PDF.

    Args:
        file_path: Path to the PDF file on disk.

    Returns:
        :class:`ExtractionResult` with all extracted holdings and metadata.
    """
    result = ExtractionResult()

    try:
        pdf: pdfplumber.PDF  # type: ignore[name-defined]
        with pdfplumber.open(file_path) as pdf:
            result.page_count = len(pdf.pages)

            # Scanned PDF detection
            if is_scanned_pdf(pdf):
                result.is_scanned = True
                logger.error("Scanned PDF detected, skipping: %s", file_path)
                result.errors.append(
                    f"Scanned PDF (no extractable text): {file_path}"
                )
                return result

            # Collect text and tables per page
            page_data: list[tuple[str, list[list[list[str | None]]]]] = []
            for page in pdf.pages:
                page_text: str = page.extract_text() or ""
                page_tables: list[list[list[str | None]]] = (
                    page.extract_tables() or []
                )
                page_data.append((page_text, page_tables))

            # Group pages into forms.  Each page starting with
            # "FORMULÁRIO CONSOLIDADO" begins a new form; otherwise
            # pages are appended to the previous form.
            form_marker = re.compile(
                r"FORMUL[AÁ]RIO\s+CONSOLIDADO", re.IGNORECASE
            )
            form_groups: list[list[tuple[str, list[list[list[str | None]]]]]] = []
            for page_text, page_tables in page_data:
                if form_marker.search(page_text):
                    form_groups.append([(page_text, page_tables)])
                elif form_groups:
                    form_groups[-1].append((page_text, page_tables))
                else:
                    form_groups.append([(page_text, page_tables)])

            for group in form_groups:
                try:
                    form_text = "\n".join(pt for pt, _ in group)
                    form_tables: list[list[list[str | None]]] = []
                    for _, pt in group:
                        form_tables.extend(pt)
                    form_result = _parse_form(form_text, form_tables)
                    result.forms.append(form_result)
                except Exception as exc:
                    msg = f"Error parsing form: {exc}"
                    logger.error(msg)
                    result.errors.append(msg)

    except Exception as exc:
        msg = f"Error opening PDF {file_path}: {exc}"
        logger.error(msg)
        result.errors.append(msg)

    return result
