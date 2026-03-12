# parsers/macro.py
import io, os, re, pdfplumber, pandas as pd
from typing import Dict, List

# Patrones (del individual)
ACC_RE      = re.compile(r"^CUENTA\s+CORRIENTE.*?NRO\.:.*", re.IGNORECASE)
DATE_RE     = re.compile(r"^\s*(\d{2}/\d{2}/\d{2,4})\b")
MONEY_RE    = re.compile(r"-?\$?\s?\d{1,3}(?:\.\d{3})*,\d{2}-?")
HEAD_DET_RE = re.compile(r"DETALLE DE MOVIMIENTO", re.IGNORECASE)

def normalize_date(tok: str) -> str:
    parts = tok.split("/")
    if len(parts) == 3 and len(parts[2]) == 2:
        parts[2] = f"20{int(parts[2]):02d}"
    return "/".join(parts[:3])

def to_number(s: str) -> float:
    if not s: return 0.0
    t = s.strip().replace("$","")
    t = t.replace(".", "").replace(",", ".")
    neg = False
    if t.startswith("-"): neg, t = True, t[1:]
    if t.endswith("-"):  neg, t = True, t[:-1]
    try: v = float(t)
    except: v = 0.0
    return -v if neg else v

def extract_lines_pdfplumber(file_like) -> List[str]:
    lines: List[str] = []
    with pdfplumber.open(file_like) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            for raw in txt.split("\n"):
                line = raw.strip()
                if line:
                    line = re.sub(r"\s{2,}", " ", line)
                    lines.append(line)
    return lines

def split_accounts(lines: List[str]) -> Dict[str, List[str]]:
    sections: Dict[str, List[str]] = {}
    current = None
    for line in lines:
        if ACC_RE.match(line):
            current = line
            sections.setdefault(current, [])
        elif current is not None:
            sections[current].append(line)
    return sections

def parse_section_to_df(header: str, lines: List[str]) -> pd.DataFrame:
    # Encontrar inicio del detalle
    start = -1
    for i, l in enumerate(lines):
        if HEAD_DET_RE.search(l):
            start = i + 1
            break
    if start == -1:
        return pd.DataFrame(columns=["Fecha","Descripción completa","Débito","Crédito","Saldo"])

    prev_saldo = None
    records: List[List] = []
    for line in lines[start:]:
        u = line.upper()
        if u.startswith("FECHA"):      # skip encabezado de tabla
            continue
        if u.startswith("SALDO FINAL"):
            break
        if u.startswith("SALDO ULTIMO EXTRACTO"):
            nums = MONEY_RE.findall(line)
            if nums:
                prev_saldo = to_number(nums[-1])
            continue

        m = DATE_RE.match(line)
        if not m:
            continue  # no parece movimiento

        fecha_tok = normalize_date(m.group(1))
        nums = MONEY_RE.findall(line)
        saldo = to_number(nums[-1]) if nums else prev_saldo
        delta = 0.0 if prev_saldo is None else (saldo - prev_saldo)
        credito = max(delta, 0.0)
        debito  = max(-delta, 0.0)

        desc = line[m.end():].strip()
        desc = MONEY_RE.sub("", desc).strip()
        desc = re.sub(r"\s{2,}", " ", desc)

        records.append([fecha_tok, desc, debito, credito, saldo])
        prev_saldo = saldo

    df = pd.DataFrame(records, columns=["Fecha","Descripción completa","Débito","Crédito","Saldo"])
    if not df.empty:
        df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%Y", errors="coerce")
    return df

def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    # 1) Extraer líneas por texto (como en la app individual; OCR queda fuera para la unificada)
    lines = extract_lines_pdfplumber(io.BytesIO(file_bytes))
    sections = split_accounts(lines)
    blocks: List[pd.DataFrame] = []
    cols = ["Fecha","Descripción completa","Débito","Crédito","Saldo"]
    for header, sec_lines in sections.items():
        df = parse_section_to_df(header, sec_lines)
        if df.empty:
            continue
        # separador por cuenta (sólo una fila de título)
        acc_type = header.split(" NRO.:")[0].strip()
        sep = pd.DataFrame([[pd.NaT, f"=== {acc_type} ===", None, None, None]], columns=cols)
        blocks.extend([sep, df])
        # fila en blanco
        blocks.append(pd.DataFrame([[pd.NaT, "", None, None, None]], columns=cols))

    out = pd.concat(blocks, ignore_index=True) if blocks else pd.DataFrame(columns=cols)
    # Adaptar a la unificada
    out = out.rename(columns={"Descripción completa": "Descripción"})
    return out[["Fecha","Descripción","Débito","Crédito","Saldo"]]

