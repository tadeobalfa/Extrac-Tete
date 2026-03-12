# parsers/nacion.py
import io, re, pdfplumber, pandas as pd
from typing import List

# Fecha al inicio (luego de normalizar prefijos raros)
DATE_RE = re.compile(r"^\s*(\d{2}/\d{2}/\d{2,4})\b")

# Montos tipo 1.234,56 o -1.234,56-
MONEY_RE = re.compile(r"-?\$?\s?\d{1,3}(?:\.\d{3})*,\d{2}-?")

SALDO_ANT_RE   = re.compile(r"^\s*SALDO\s+ANTERIOR\b", re.IGNORECASE)
SALDO_FINAL_RE = re.compile(r"^\s*SALDO\s+FINAL\b", re.IGNORECASE)

# Detecta basura tipo líneas negras/guiones bajos al inicio
LEADING_GARBAGE_BEFORE_DATE = re.compile(r"^[^\d]{1,40}(?=\d{2}/\d{2}/\d{2,4}\b)")

def normalize_date(tok: str) -> str:
    parts = tok.split("/")
    if len(parts) == 3 and len(parts[2]) == 2:
        parts[2] = f"20{int(parts[2]):02d}"
    return "/".join(parts[:3])

def to_number(s: str) -> float:
    if not s:
        return 0.0
    t = s.strip().replace("$", "").replace(" ", "")
    neg = False
    if t.startswith("-"):
        neg, t = True, t[1:]
    if t.endswith("-"):
        neg, t = True, t[:-1]
    t = t.replace(".", "").replace(",", ".")
    try:
        v = float(t)
    except Exception:
        v = 0.0
    return -v if neg else v

def _clean_line(raw: str) -> str:
    """
    - Colapsa espacios
    - Saca prefijos basura antes de una fecha (ej: '____ 03/01/25 ...')
    """
    line = (raw or "").strip()
    if not line:
        return ""
    line = re.sub(r"\s{2,}", " ", line)
    line = LEADING_GARBAGE_BEFORE_DATE.sub("", line).strip()
    return line

def extract_lines(file_like) -> List[str]:
    lines: List[str] = []
    with pdfplumber.open(file_like) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            for raw in txt.split("\n"):
                line = _clean_line(raw)
                if line:
                    lines.append(line)
    return lines

def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    lines = extract_lines(io.BytesIO(file_bytes))
    records = []
    prev_saldo = None
    started = False

    for line in lines:
        # Buscar inicio: SALDO ANTERIOR
        if not started and SALDO_ANT_RE.match(line):
            nums = MONEY_RE.findall(line)
            if nums:
                prev_saldo = to_number(nums[-1])
                started = True
            continue

        if not started:
            continue

        # Corte: SALDO FINAL
        if SALDO_FINAL_RE.match(line):
            break

        m = DATE_RE.match(line)
        if not m:
            continue

        # IMPORTANTÍSIMO:
        # Si no hay al menos 2 montos (importe + saldo), NO ES MOVIMIENTO.
        # Esto elimina TRANSPORTE, PAGINA, encabezados/pies, etc.
        nums = MONEY_RE.findall(line)
        if len(nums) < 2:
            continue

        fecha_tok = normalize_date(m.group(1))
        saldo = to_number(nums[-1])

        delta = 0.0 if prev_saldo is None else (saldo - prev_saldo)
        credito = max(delta, 0.0)
        debito  = max(-delta, 0.0)

        desc = line[m.end():].strip()
        desc = MONEY_RE.sub("", desc).strip()
        desc = re.sub(r"\s{2,}", " ", desc).strip(" ,;")

        records.append([fecha_tok, desc, debito, credito, saldo])
        prev_saldo = saldo

    df = pd.DataFrame(records, columns=["Fecha", "Descripción completa", "Débito", "Crédito", "Saldo"])
    if not df.empty:
        df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%Y", errors="coerce")

    # Adaptar a la unificada
    df = df.rename(columns={"Descripción completa": "Descripción"})
    return df[["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]]

