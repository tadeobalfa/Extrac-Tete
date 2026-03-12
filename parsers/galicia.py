import io
import re
import os
import pandas as pd
import pdfplumber

# ====== Config de límites ======
MAX_PAGES = int(os.getenv("EXTRACTOS_MAX_PAGES", "0"))
MAX_LINES = int(os.getenv("EXTRACTOS_MAX_LINES", "60000"))

# Fecha al INICIO de la línea
DATE_START = re.compile(r"^\s*(\d{2}/\d{2}/\d{2,4})\b")

# Importes estilo AR
MONEY_RE = re.compile(r"[−-]?\$?\s?\d{1,3}(?:\.\d{3})*,\d{2}-?")

MAX_LINE_LEN = 220

HEADER_FOOTER_PAT = re.compile(
    r"""(?ix)
    ^\s*(Página\s+\d+\s*/\s*\d+|
         Resumen\s+de\s+Cuenta|
         CUIT\s+del\s+Responsable|
         IVA:\s*Responsable|
         Cantidad\s+de\s+cotitulares|
         Datos\s+de\s+la\s+cuenta|
         Tipo\s+de\s+cuenta|Número\s+de\s+cuenta|CBU|Período\s+de\s+movimientos|
         Saldos|Disponés\s+de\s+30\s+días
    )
    """,
    re.UNICODE,
)

# ====== NUEVO: secciones donde hay que cortar el último movimiento ======
STOP_SECTION_PAT = re.compile(
    r"""(?ix)
    ^\s*(
        Total\b|
        Consolidado\s+de\s+retención\s+de\s+impuestos\b|
        Consolidado\b|
        Importe\b|
        PERIODO\s+COMPRENDIDO\s+ENTRE\b|
        TOTAL\s+IMPUESTO\s+I\.?V\.?A\.?\s+SOBRE\s+DEBITOS\b|
        TOTAL\s+RETENCION\s+IMPUESTO\s+LEY\s+25\.?413\s+SOBRE\s+CREDITOS\b|
        TOTAL\s+RETENCION\s+IMPUESTO\s+LEY\s+25\.?413\s+SOBRE\s+DEBITOS\b|
        TOTAL\s+MENSUAL\s+RETENCION\s+IMPUESTO\s+LEY\s+25\.?413\b|
        Los\s+depósitos\s+en\s+pesos\b|
        Al\s+completa(?:r)?\s+esta\s+hoja\b|
        Canales\s+de\s+atención\b|
        Ingresá\s+a\s+"?Ayudas\s+para\s+tus\s+dudas"?\b|
        Chateá\s+por\s+Whatsapp\b|
        Llamanos\s+a\s+Fonobanco\b|
        Usted\s+puede\s+consultar\s+el\s+"?Régimen\s+de\s+Transparencia"?\b|
        https?://|
        www\.|
        bancogalicia\.com\b|
        \d{15,}[A-Z]?\b
    )
    """,
    re.UNICODE,
)


def _to_float(tok: str) -> float:
    if not tok:
        return 0.0
    t = tok.strip().replace("−", "-").replace("$", "")
    neg = False
    if t.endswith("-"):
        neg, t = True, t[:-1]
    if t.startswith("-"):
        neg, t = True, t[1:]
    t = t.replace(".", "").replace(",", ".")
    try:
        v = float(t)
    except Exception:
        v = 0.0
    return -v if neg else v


def _clean_desc_line(line: str, first_date: str | None) -> str:
    if not line:
        return ""
    out = MONEY_RE.sub("", line).strip()
    if first_date and out.startswith(first_date):
        out = out[len(first_date):]
    out = re.sub(r"\s{2,}", " ", out).strip(" |-")
    return out


def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    rows = []
    cur_date = None
    cur_desc_lines = []
    cur_amounts = []
    line_count = 0
    page_count = 0

    def flush():
        nonlocal cur_date, cur_desc_lines, cur_amounts, rows

        if not cur_date:
            cur_desc_lines, cur_amounts = [], []
            return

        saldo_tok = cur_amounts[-1] if cur_amounts else ""
        if not saldo_tok:
            cur_date = None
            cur_desc_lines, cur_amounts = [], []
            return

        otros = cur_amounts[:-1]
        deb = 0.0
        cred = 0.0

        for tok in otros:
            val = _to_float(tok)
            if val < 0 and deb == 0.0:
                deb = abs(val)
            elif val > 0 and cred == 0.0:
                cred = val

        clean = []
        for i, ln in enumerate(cur_desc_lines):
            cl = _clean_desc_line(ln, cur_date if i == 0 else None)
            if cl:
                clean.append(cl)

        desc = " | ".join(clean)
        rows.append([cur_date, desc, deb, cred, _to_float(saldo_tok)])

        cur_date = None
        cur_desc_lines, cur_amounts = [], []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            page_count += 1
            if MAX_PAGES and page_count > MAX_PAGES:
                break

            text = page.extract_text() or ""

            for raw in text.split("\n"):
                line_count += 1
                if line_count > MAX_LINES:
                    flush()
                    break

                line = (raw or "").rstrip()
                if not line:
                    continue

                if len(line) > MAX_LINE_LEN:
                    continue

                if HEADER_FOOTER_PAT.search(line):
                    continue

                # ====== NUEVO: cortar movimiento actual si arranca una sección de resumen/boilerplate ======
                if STOP_SECTION_PAT.search(line):
                    if cur_date:
                        flush()
                    continue

                m = DATE_START.match(line)
                if m:
                    flush()
                    cur_date = m.group(1)
                    cur_desc_lines = [line]
                    cur_amounts = MONEY_RE.findall(line)
                else:
                    if cur_date:
                        cur_desc_lines.append(line)
                        found = MONEY_RE.findall(line)
                        if found:
                            cur_amounts.extend(found)

            if line_count > MAX_LINES:
                break

    flush()

    df = pd.DataFrame(rows, columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo"])
    if not df.empty:
        df["Fecha"] = pd.to_datetime(
            df["Fecha"], format="%d/%m/%y", errors="coerce"
        ).fillna(
            pd.to_datetime(df["Fecha"], format="%d/%m/%Y", errors="coerce")
        )

    return df