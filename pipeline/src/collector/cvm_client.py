"""CVM data client for downloading cadastral and document data."""

import csv
import io
import logging
import urllib.request
import zipfile
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class CompanyRecord:
    """Raw company record from CVM cadastral CSV."""

    cvm_code: str
    name: str
    cnpj: str
    sector: str | None
    subsector: str | None
    is_active: bool


def fetch_cadastral_csv(
    base_url: str = "https://dados.cvm.gov.br",
) -> str:
    """Download the CVM cadastral CSV for open companies.

    The CSV is encoded in ISO-8859-1 with semicolon delimiter.

    Args:
        base_url: Base URL for CVM data portal.

    Returns:
        Raw CSV content as string.
    """
    url = f"{base_url}/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"
    logger.info("Downloading cadastral CSV from %s", url)

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "InsiderTrack/1.0"},
    )
    with urllib.request.urlopen(req, timeout=60) as response:
        raw_bytes: bytes = response.read()

    content = raw_bytes.decode("iso-8859-1")
    logger.info("Downloaded cadastral CSV (%d bytes)", len(raw_bytes))
    return content


def _parse_sector(setor_ativ: str) -> tuple[str | None, str | None]:
    """Parse sector and subsector from SETOR_ATIV field.

    CVM uses formats like "Intermediários Financeiros / Bancos".
    We split on " / " to separate sector from subsector when possible.

    Args:
        setor_ativ: Raw sector activity string from CVM.

    Returns:
        Tuple of (sector, subsector).
    """
    if not setor_ativ or not setor_ativ.strip():
        return None, None

    setor_ativ = setor_ativ.strip()

    if " / " in setor_ativ:
        parts = setor_ativ.split(" / ", 1)
        return parts[0].strip(), parts[1].strip()

    return setor_ativ, None


def parse_cadastral_csv(csv_content: str) -> list[CompanyRecord]:
    """Parse CVM cadastral CSV content into CompanyRecord list.

    The CSV uses semicolon delimiter and contains company registration data.

    Args:
        csv_content: Raw CSV string content.

    Returns:
        List of parsed company records.
    """
    reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")

    records: list[CompanyRecord] = []
    for row in reader:
        cvm_code = row.get("CD_CVM", "").strip()
        name = row.get("DENOM_SOCIAL", "").strip()
        cnpj = row.get("CNPJ_CIA", "").strip()
        setor_ativ = row.get("SETOR_ATIV", "").strip()
        sit = row.get("SIT", "").strip()

        if not cvm_code or not name:
            continue

        sector, subsector = _parse_sector(setor_ativ)
        is_active = sit.upper() == "ATIVO"

        records.append(
            CompanyRecord(
                cvm_code=cvm_code,
                name=name,
                cnpj=cnpj,
                sector=sector,
                subsector=subsector,
                is_active=is_active,
            )
        )

    logger.info("Parsed %d company records from cadastral CSV", len(records))
    return records


def fetch_and_parse_companies(
    base_url: str = "https://dados.cvm.gov.br",
) -> list[CompanyRecord]:
    """Fetch and parse CVM cadastral data in one step.

    Args:
        base_url: Base URL for CVM data portal.

    Returns:
        List of parsed company records.
    """
    csv_content = fetch_cadastral_csv(base_url)
    return parse_cadastral_csv(csv_content)


# ---------------------------------------------------------------------------
# Document CSV (annual ZIP with insider trading filings metadata)
# ---------------------------------------------------------------------------

# CVM IPE category for insider trading position disclosures.
INSIDER_TRADING_CATEGORY = "Valores Mobiliários Negociados e Detidos"


@dataclass
class DocumentRecord:
    """A CVM document metadata record from the IPE CSV."""

    cvm_code: str
    cnpj: str
    reference_date: str  # YYYY-MM-DD
    delivery_date: str  # YYYY-MM-DD
    document_url: str
    category: str
    document_type: str
    status: str
    version: str


def fetch_document_zip(
    year: int,
    base_url: str = "https://dados.cvm.gov.br",
) -> str:
    """Download annual CVM IPE ZIP and extract the CSV content.

    The ZIP is at a well-known path and contains a single CSV file
    encoded in ISO-8859-1 with semicolon delimiter.

    Args:
        year: The year to download (e.g. 2025).
        base_url: Base URL for CVM data portal.

    Returns:
        Raw CSV content as string.
    """
    url = f"{base_url}/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.zip"
    logger.info("Downloading document ZIP from %s", url)

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "InsiderTrack/1.0"},
    )
    with urllib.request.urlopen(req, timeout=120) as response:
        raw_bytes: bytes = response.read()

    logger.info("Downloaded document ZIP (%d bytes)", len(raw_bytes))

    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
        csv_filename = f"ipe_cia_aberta_{year}.csv"
        # Fallback: pick first CSV if exact name doesn't match
        names = zf.namelist()
        if csv_filename not in names:
            csv_files = [n for n in names if n.endswith(".csv")]
            if not csv_files:
                raise ValueError(f"No CSV file found in ZIP for year {year}")
            csv_filename = csv_files[0]
            logger.warning(
                "Expected %s not found, using %s",
                f"ipe_cia_aberta_{year}.csv",
                csv_filename,
            )

        raw_csv_bytes = zf.read(csv_filename)

    content = raw_csv_bytes.decode("iso-8859-1")
    logger.info("Extracted CSV %s (%d bytes)", csv_filename, len(raw_csv_bytes))
    return content


def _get_field(row: dict[str, str], *candidates: str) -> str:
    """Return the first non-empty value found among candidate column names."""
    for key in candidates:
        val = row.get(key, "").strip()
        if val:
            return val
    return ""


def parse_document_csv(
    csv_content: str,
    category_filter: str = INSIDER_TRADING_CATEGORY,
) -> list[DocumentRecord]:
    """Parse IPE CSV and filter for insider trading documents.

    Supports both legacy column names (CATEG_DOC, CD_CVM, etc.) and the
    current CVM format (Categoria, Codigo_CVM, etc.).

    Args:
        csv_content: Raw CSV string (ISO-8859-1 decoded).
        category_filter: Only keep rows whose category contains this string.

    Returns:
        List of DocumentRecord for matching documents.
    """
    reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")

    records: list[DocumentRecord] = []
    total_rows = 0
    for row in reader:
        total_rows += 1
        categ = _get_field(row, "CATEG_DOC", "Categoria")
        if category_filter and category_filter.lower() not in categ.lower():
            continue

        cvm_code = _get_field(row, "CD_CVM", "Codigo_CVM")
        cnpj = _get_field(row, "CNPJ_CIA", "CNPJ_Companhia")
        dt_refer = _get_field(row, "DT_REFER", "Data_Referencia")
        dt_receb = _get_field(row, "DT_RECEB", "Data_Entrega")
        link_doc = _get_field(row, "LINK_DOC", "Link_Download")
        tp_doc = _get_field(row, "TP_DOC", "Tipo")
        sit_doc = _get_field(row, "SIT_DOC", "Tipo_Apresentacao")
        versao = _get_field(row, "VERSAO", "Versao")

        if not cvm_code or not link_doc:
            continue

        records.append(
            DocumentRecord(
                cvm_code=cvm_code,
                cnpj=cnpj,
                reference_date=dt_refer,
                delivery_date=dt_receb,
                document_url=link_doc,
                category=categ,
                document_type=tp_doc,
                status=sit_doc,
                version=versao,
            )
        )

    logger.info(
        "Parsed %d insider trading documents from %d total rows",
        len(records),
        total_rows,
    )
    return records


def fetch_and_parse_documents(
    year: int,
    base_url: str = "https://dados.cvm.gov.br",
) -> list[DocumentRecord]:
    """Fetch and parse CVM document data for a given year.

    Downloads the annual IPE ZIP, extracts the CSV, and filters
    for insider trading category documents.

    Args:
        year: The year to fetch documents for.
        base_url: Base URL for CVM data portal.

    Returns:
        List of insider trading document records.
    """
    csv_content = fetch_document_zip(year, base_url)
    return parse_document_csv(csv_content)


# ---------------------------------------------------------------------------
# Material Facts CSV (annual ZIP with "Fatos Relevantes")
# ---------------------------------------------------------------------------

# CVM IPE category for material facts disclosures.
MATERIAL_FACTS_CATEGORY = "Fato Relevante"


@dataclass
class MaterialFactRecord:
    """A CVM material fact record from the IPE CSV."""

    cvm_code: str
    cnpj: str
    reference_date: str  # YYYY-MM-DD
    delivery_date: str  # YYYY-MM-DD
    category: str
    subject: str
    source_url: str
    protocol: str
    version: str
    status: str


def fetch_material_facts_csv(
    year: int,
    base_url: str = "https://dados.cvm.gov.br",
) -> str:
    """Download annual CVM IPE ZIP and extract material facts CSV.

    Material facts ("Fatos Relevantes") share the IPE CSV with insider
    trading documents; they are filtered by the MATERIAL_FACTS_CATEGORY.
    We reuse the same IPE ZIP download.

    Args:
        year: The year to download (e.g. 2025).
        base_url: Base URL for CVM data portal.

    Returns:
        Raw CSV content as string.
    """
    return fetch_document_zip(year, base_url)


def parse_material_facts_csv(
    csv_content: str,
    category_filter: str = MATERIAL_FACTS_CATEGORY,
) -> list[MaterialFactRecord]:
    """Parse IPE CSV and filter for material fact documents.

    Supports both legacy column names and the current CVM format.

    Args:
        csv_content: Raw CSV string (ISO-8859-1 decoded).
        category_filter: Only keep rows whose category contains this string.

    Returns:
        List of MaterialFactRecord for matching documents.
    """
    reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")

    records: list[MaterialFactRecord] = []
    total_rows = 0
    for row in reader:
        total_rows += 1
        categ = _get_field(row, "CATEG_DOC", "Categoria")
        if category_filter and category_filter.lower() not in categ.lower():
            continue

        cvm_code = _get_field(row, "CD_CVM", "Codigo_CVM")
        cnpj = _get_field(row, "CNPJ_CIA", "CNPJ_Companhia")
        dt_refer = _get_field(row, "DT_REFER", "Data_Referencia")
        dt_receb = _get_field(row, "DT_RECEB", "Data_Entrega")
        link_doc = _get_field(row, "LINK_DOC", "Link_Download")
        assunto = _get_field(row, "ASSUNTO", "Assunto")
        protocolo = _get_field(row, "PROTOCOLO", "Protocolo_Entrega", "Protocolo")
        versao = _get_field(row, "VERSAO", "Versao")
        sit_doc = _get_field(row, "SIT_DOC", "Tipo_Apresentacao")

        if not cvm_code or not protocolo:
            continue

        records.append(
            MaterialFactRecord(
                cvm_code=cvm_code,
                cnpj=cnpj,
                reference_date=dt_refer,
                delivery_date=dt_receb,
                category=categ,
                subject=assunto,
                source_url=link_doc,
                protocol=protocolo,
                version=versao,
                status=sit_doc,
            )
        )

    logger.info(
        "Parsed %d material fact records from %d total rows",
        len(records),
        total_rows,
    )
    return records


def fetch_and_parse_material_facts(
    year: int,
    base_url: str = "https://dados.cvm.gov.br",
) -> list[MaterialFactRecord]:
    """Fetch and parse CVM material facts for a given year.

    Downloads the annual IPE ZIP, extracts the CSV, and filters
    for material fact ("Fato Relevante") documents.

    Args:
        year: The year to fetch material facts for.
        base_url: Base URL for CVM data portal.

    Returns:
        List of material fact records.
    """
    csv_content = fetch_material_facts_csv(year, base_url)
    return parse_material_facts_csv(csv_content)


# ---------------------------------------------------------------------------
# Financial Statements CSV (DFP annual / ITR quarterly)
# ---------------------------------------------------------------------------

# Statement types we extract from CVM DFP/ITR ZIPs.
# Each corresponds to a CSV file inside the ZIP.
FINANCIAL_STATEMENT_TYPES = ["BPA", "BPP", "DRE", "DFC_MI"]


@dataclass
class FinancialStatementRecord:
    """A CVM financial statement line item."""

    cvm_code: str
    reference_date: str  # YYYY-MM-DD
    statement_type: str  # BPA, BPP, DRE, DFC_MI
    account_code: str
    account_name: str
    value: str  # raw string, converted to Decimal downstream
    currency: str
    version: str


def fetch_financial_zip(
    year: int,
    report_type: str = "DFP",
    base_url: str = "https://dados.cvm.gov.br",
) -> bytes:
    """Download annual CVM financial statement ZIP.

    Args:
        year: The year to download.
        report_type: "DFP" (annual) or "ITR" (quarterly).
        base_url: Base URL for CVM data portal.

    Returns:
        Raw ZIP bytes.
    """
    rt = report_type.lower()
    RT = report_type.upper()
    url = f"{base_url}/dados/CIA_ABERTA/DOC/{RT}/DADOS/{rt}_cia_aberta_{year}.zip"
    logger.info("Downloading financial ZIP from %s", url)

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "InsiderTrack/1.0"},
    )
    with urllib.request.urlopen(req, timeout=120) as response:
        raw_bytes: bytes = response.read()

    logger.info("Downloaded financial ZIP (%d bytes)", len(raw_bytes))
    return raw_bytes


def _parse_financial_csv(
    csv_content: str,
    statement_type: str,
) -> list[FinancialStatementRecord]:
    """Parse a single financial statement CSV into records."""
    reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")

    records: list[FinancialStatementRecord] = []
    for row in reader:
        cvm_code = _get_field(row, "CD_CVM", "Codigo_CVM").lstrip("0") or "0"
        dt_refer = _get_field(row, "DT_REFER", "Data_Referencia")
        cd_conta = _get_field(row, "CD_CONTA", "Codigo_Conta")
        ds_conta = _get_field(row, "DS_CONTA", "Descricao_Conta")
        vl_conta = _get_field(row, "VL_CONTA", "Valor_Conta")
        moeda = _get_field(row, "MOEDA_ORIG", "Moeda") or "BRL"
        versao = _get_field(row, "VERSAO", "Versao")

        if not cvm_code or not cd_conta or not dt_refer:
            continue

        records.append(
            FinancialStatementRecord(
                cvm_code=cvm_code,
                reference_date=dt_refer,
                statement_type=statement_type,
                account_code=cd_conta,
                account_name=ds_conta,
                value=vl_conta,
                currency=moeda,
                version=versao,
            )
        )

    return records


def parse_financial_zip(
    zip_bytes: bytes,
    year: int,
    report_type: str = "DFP",
    statement_types: list[str] | None = None,
) -> list[FinancialStatementRecord]:
    """Extract and parse financial statement CSVs from a ZIP.

    Args:
        zip_bytes: Raw ZIP content.
        year: Year (used to locate CSV filenames).
        report_type: "DFP" or "ITR".
        statement_types: Which statement types to extract.
            Defaults to FINANCIAL_STATEMENT_TYPES.

    Returns:
        Combined list of all financial statement records.
    """
    if statement_types is None:
        statement_types = FINANCIAL_STATEMENT_TYPES

    rt = report_type.lower()
    all_records: list[FinancialStatementRecord] = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        for st in statement_types:
            csv_filename = f"{rt}_cia_aberta_{st}_con_{year}.csv"
            # Try case-insensitive match
            match = None
            for n in names:
                if n.lower() == csv_filename.lower():
                    match = n
                    break
            if match is None:
                logger.warning("CSV %s not found in ZIP, skipping", csv_filename)
                continue

            raw_csv_bytes = zf.read(match)
            content = raw_csv_bytes.decode("iso-8859-1")
            records = _parse_financial_csv(content, st)
            logger.info(
                "Parsed %d records from %s", len(records), csv_filename
            )
            all_records.extend(records)

    logger.info(
        "Total financial statement records from %s %d: %d",
        report_type,
        year,
        len(all_records),
    )
    return all_records


def fetch_and_parse_financial_statements(
    year: int,
    report_type: str = "DFP",
    base_url: str = "https://dados.cvm.gov.br",
    statement_types: list[str] | None = None,
) -> list[FinancialStatementRecord]:
    """Fetch and parse CVM financial statements for a given year.

    Downloads the DFP or ITR ZIP, extracts statement CSVs, and parses
    them into FinancialStatementRecord objects.

    Args:
        year: The year to fetch.
        report_type: "DFP" (annual) or "ITR" (quarterly).
        base_url: Base URL for CVM data portal.
        statement_types: Which statement types to extract.

    Returns:
        List of financial statement records.
    """
    zip_bytes = fetch_financial_zip(year, report_type, base_url)
    return parse_financial_zip(zip_bytes, year, report_type, statement_types)


# ---------------------------------------------------------------------------
# Dividends CSV (FCA — Formulário Cadastral, proventos em dinheiro)
# ---------------------------------------------------------------------------


@dataclass
class DividendRecord:
    """A CVM dividend (provento) record from the FCA CSV."""

    cvm_code: str
    ex_date: str  # YYYY-MM-DD or empty
    payment_date: str  # YYYY-MM-DD or empty
    record_date: str  # YYYY-MM-DD or empty
    dividend_type: str  # Dividendo, JCP, etc.
    value_per_share: str  # raw string, converted downstream
    total_value: str  # raw string, converted downstream
    currency: str
    source_url: str
    version: str


def fetch_dividends_zip(
    year: int,
    base_url: str = "https://dados.cvm.gov.br",
) -> bytes:
    """Download annual CVM FCA ZIP containing proventos CSV.

    Args:
        year: The year to download.
        base_url: Base URL for CVM data portal.

    Returns:
        Raw ZIP bytes.
    """
    url = f"{base_url}/dados/CIA_ABERTA/DOC/FCA/DADOS/fca_cia_aberta_{year}.zip"
    logger.info("Downloading FCA ZIP from %s", url)

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "InsiderTrack/1.0"},
    )
    with urllib.request.urlopen(req, timeout=120) as response:
        raw_bytes: bytes = response.read()

    logger.info("Downloaded FCA ZIP (%d bytes)", len(raw_bytes))
    return raw_bytes


def parse_dividends_csv(
    csv_content: str,
) -> list[DividendRecord]:
    """Parse FCA proventos CSV into DividendRecord list.

    Supports both legacy and current CVM column names.

    Args:
        csv_content: Raw CSV string (ISO-8859-1 decoded).

    Returns:
        List of DividendRecord.
    """
    reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")

    records: list[DividendRecord] = []
    for row in reader:
        cvm_code = _get_field(row, "CD_CVM", "Codigo_CVM")
        if not cvm_code:
            continue

        dt_ex = _get_field(row, "Data_Ex", "DT_EX", "DT_EX_DIVIDENDO")
        dt_pagamento = _get_field(
            row, "Data_Pagamento", "DT_PAGAMENTO", "DT_PAG"
        )
        dt_aprovacao = _get_field(
            row, "Data_Aprovacao", "DT_APROVACAO", "Data_Registro", "DT_REGISTRO"
        )
        tp_provento = _get_field(
            row, "Tipo_Provento", "TP_PROVENTO", "Descricao_Provento", "DS_PROVENTO"
        )
        vl_provento = _get_field(
            row, "Valor_Provento", "VL_PROVENTO", "VL_PROVENTO_ACAO"
        )
        vl_total = _get_field(
            row, "Valor_Total", "VL_TOTAL", "VL_TOTAL_PROVENTO"
        )
        moeda = _get_field(row, "MOEDA", "Moeda") or "BRL"
        versao = _get_field(row, "VERSAO", "Versao")

        if not dt_ex and not dt_pagamento:
            continue

        records.append(
            DividendRecord(
                cvm_code=cvm_code,
                ex_date=dt_ex,
                payment_date=dt_pagamento,
                record_date=dt_aprovacao,
                dividend_type=tp_provento or "Dividendo",
                value_per_share=vl_provento,
                total_value=vl_total,
                currency=moeda,
                source_url="",
                version=versao,
            )
        )

    logger.info("Parsed %d dividend records from FCA CSV", len(records))
    return records


def fetch_and_parse_dividends(
    year: int,
    base_url: str = "https://dados.cvm.gov.br",
) -> list[DividendRecord]:
    """Fetch and parse CVM dividend data for a given year.

    Downloads the FCA ZIP, extracts the proventos CSV, and parses it
    into DividendRecord objects.

    Args:
        year: The year to fetch dividends for.
        base_url: Base URL for CVM data portal.

    Returns:
        List of dividend records.
    """
    zip_bytes = fetch_dividends_zip(year, base_url)

    # Extract proventos CSV from FCA ZIP
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        # Look for proventos CSV (case-insensitive)
        prov_filename = None
        for n in names:
            if "prov_dinheiro" in n.lower() and n.lower().endswith(".csv"):
                prov_filename = n
                break

        if prov_filename is None:
            logger.warning(
                "No prov_dinheiro CSV found in FCA ZIP for year %d. "
                "Available files: %s",
                year,
                names,
            )
            return []

        raw_csv_bytes = zf.read(prov_filename)

    content = raw_csv_bytes.decode("iso-8859-1")
    logger.info("Extracted %s (%d bytes)", prov_filename, len(raw_csv_bytes))
    return parse_dividends_csv(content)


# ---------------------------------------------------------------------------
# Insider Positions CSV (FRE — Formulário de Referência, posição acionária)
# ---------------------------------------------------------------------------


@dataclass
class InsiderPositionRecord:
    """A CVM insider position record from the FRE position CSV."""

    cvm_code: str
    insider_name: str
    insider_group: str  # Tipo/category of the shareholder
    cpf_cnpj: str
    reference_date: str  # YYYY-MM-DD
    asset_type: str  # ON, PN, etc.
    asset_description: str
    quantity: str  # raw string, converted downstream
    total_value: str  # raw string, converted downstream
    version: str


def fetch_positions_zip(
    year: int,
    base_url: str = "https://dados.cvm.gov.br",
) -> bytes:
    """Download annual CVM FRE ZIP containing insider position data.

    Args:
        year: The year to download.
        base_url: Base URL for CVM data portal.

    Returns:
        Raw ZIP bytes.
    """
    url = f"{base_url}/dados/CIA_ABERTA/DOC/FRE/DADOS/fre_cia_aberta_{year}.zip"
    logger.info("Downloading FRE ZIP from %s", url)

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "InsiderTrack/1.0"},
    )
    with urllib.request.urlopen(req, timeout=120) as response:
        raw_bytes: bytes = response.read()

    logger.info("Downloaded FRE ZIP (%d bytes)", len(raw_bytes))
    return raw_bytes


def parse_positions_csv(
    csv_content: str,
) -> list[InsiderPositionRecord]:
    """Parse FRE position CSV into InsiderPositionRecord list.

    Supports both legacy and current CVM column names.

    Args:
        csv_content: Raw CSV string (ISO-8859-1 decoded).

    Returns:
        List of InsiderPositionRecord.
    """
    reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")

    records: list[InsiderPositionRecord] = []
    for row in reader:
        # Support CD_CVM, Codigo_CVM, or CNPJ_Companhia for company lookup
        cvm_code = _get_field(row, "CD_CVM", "Codigo_CVM")
        if cvm_code:
            cvm_code = cvm_code.lstrip("0") or "0"
        else:
            # FRE posicao_acionaria has no CVM code — use CNPJ for lookup
            cvm_code = _get_field(row, "CNPJ_Companhia", "CNPJ_CIA")
        if not cvm_code:
            continue

        nome = _get_field(
            row,
            "Acionista",
            "Nome_Controlador",
            "Nome_Acionista",
            "NOME_CONTROLADOR",
        )
        if not nome:
            continue

        tipo = _get_field(
            row,
            "Acionista_Controlador",
            "Tipo_Controlador",
            "Tipo_Acionista",
            "TP_CONTROLADOR",
        )
        # Map S/N to descriptive text
        if tipo == "S":
            tipo = "Controlador"
        elif tipo == "N":
            tipo = "Não Controlador"

        cpf_cnpj = _get_field(
            row,
            "CPF_CNPJ_Acionista",
            "CPF_CNPJ_Controlador",
            "CPF_CNPJ",
        )
        dt_refer = _get_field(row, "DT_REFER", "Data_Referencia")

        # Position data — FRE has separate ON/PN columns
        qty_on = _get_field(row, "Quantidade_Acao_Ordinaria_Circulacao", "Quantidade_Acoes")
        qty_pn = _get_field(row, "Quantidade_Acao_Preferencial_Circulacao")
        qty_total = _get_field(row, "Quantidade_Total_Acoes_Circulacao", "QTD_ACOES", "Quantidade")

        # Use total if available, else ON
        quantidade = qty_total or qty_on or "0"
        especie = "Ações" if qty_on else ""
        descricao = _get_field(row, "Descricao_Acao", "DS_ACAO", "Descricao") or "Ações"

        valor = _get_field(
            row,
            "Valor_Total",
            "VL_TOTAL",
            "Valor",
        )
        versao = _get_field(row, "VERSAO", "Versao")

        if not dt_refer:
            continue

        records.append(
            InsiderPositionRecord(
                cvm_code=cvm_code,
                insider_name=nome,
                insider_group=tipo,
                cpf_cnpj=cpf_cnpj,
                reference_date=dt_refer,
                asset_type=especie or "",
                asset_description=descricao,
                quantity=quantidade,
                total_value=valor,
                version=versao,
            )
        )

    logger.info("Parsed %d insider position records from FRE CSV", len(records))
    return records


def fetch_and_parse_positions(
    year: int,
    base_url: str = "https://dados.cvm.gov.br",
) -> list[InsiderPositionRecord]:
    """Fetch and parse CVM insider position data for a given year.

    Downloads the FRE ZIP, extracts the position CSV, and parses it
    into InsiderPositionRecord objects.

    Args:
        year: The year to fetch positions for.
        base_url: Base URL for CVM data portal.

    Returns:
        List of insider position records.
    """
    zip_bytes = fetch_positions_zip(year, base_url)

    # Extract position CSV from FRE ZIP
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        # Look for position CSV (case-insensitive)
        pos_filename = None
        for n in names:
            nl = n.lower()
            if nl.endswith(".csv") and (
                "posicao_acionaria" in nl
                or "controlador" in nl
            ):
                pos_filename = n
                break

        if pos_filename is None:
            logger.warning(
                "No position CSV found in FRE ZIP for year %d. "
                "Available files: %s",
                year,
                names,
            )
            return []

        raw_csv_bytes = zf.read(pos_filename)

    content = raw_csv_bytes.decode("iso-8859-1")
    logger.info("Extracted %s (%d bytes)", pos_filename, len(raw_csv_bytes))
    return parse_positions_csv(content)
