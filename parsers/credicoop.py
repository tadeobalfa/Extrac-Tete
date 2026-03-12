# parsers/credicoop.py
import io
import re
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
import pdfplumber

# === Reglas del extractor Credicoop (portadas de tu versión estable) ===
CREDIT_KEYWORDS = [
    "TII DE OT BCO", "PAGO CT PEI", "PAGO A COMERCIOS PRISMA",
    "PAVICR", "PAVIDE", "PAMADE", "PAPREP"
]
DEBIT_KEYWORDS = [
    "RECAUD. SIRCREB", "TRANSF.INMEDIATA", "IMPUESTO LEY 25.413",
    "COMISION POR TRANSFERENCIA", "I.V.A.", "SUSCRIPCION",
    "NUEVO SISTEMA DE CREDITOS", "DEBITO", "DÉBITO"
]

FECHA_RE      = re.compile(r"^\d{2}/\d{2}/\d{2}$")
FECHA_LINE_RE = re.compile(r"^\d{2}/\d{2}/\d{2}\b")
AMOUNT_RE     = re.compile(r"\d{1,3}(?:\.\d{3})*,\d{2}")

def _to_float_ar(txt: Optional[str]) -> float:
    if not txt: return 0.0
    s = str(txt).strip()
    if s == "": return 0.0
    s = s.replace(".", "").replace(",", ".")
    try: return float(s)
    except: return 0.0

def _classify(desc_upper: str) -> str:
    for k in CREDIT_KEYWORDS:
        if k in desc_upper: return "C"
    for k in DEBIT_KEYWORDS:
        if k in desc_upper: return "D"
    return "C"  # por defecto, conservador hacia crédito

def _normalize_fecha(fecha_str: str) -> pd.Timestamp:
    # dd/mm/aa → fecha sin hora
    return pd.to_datetime(fecha_str, format="%d/%m/%y", errors="coerce")

def _extract_table_rows(page) -> list:
    settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "snap_tolerance": 3,
        "intersection_tolerance": 3,
        "join_tolerance": 3,
        "edge_min_length": 3,
        "min_words_vertical": 1,
        "min_words_horizontal": 1,
    }
    tbl = page.extract_table(settings)
    if tbl: return tbl
    tbl2 = page.extract_table()
    return tbl2 or []

def _parse_pdf_structured(file_bytes: bytes) -> Tuple[pd.DataFrame, Optional[float]]:
    movimientos = []
    saldo_inicial = None
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            rows = _extract_table_rows(page)
            if not rows: 
                continue

            # Buscar encabezado
            header_idx = None
            for idx, r in enumerate(rows):
                if not r: 
                    continue
                joined = " ".join([str(x) for x in r if x])
                if "FECHA" in joined.upper():
                    header_idx = idx
                    break

            data_rows = rows[(header_idx + 1) if header_idx is not None else 0:]

            for raw in data_rows:
                if not raw: 
                    continue
                row = [str(x or "").strip() for x in raw]
                if len(row) < 6:
                    row += [""] * (6 - len(row))
                fecha, combte, desc, deb, cre, sal = row[:6]

                if not fecha and not desc: 
                    continue
                if "PAGINA" in " ".join(row).upper(): 
                    continue

                # Saldo anterior (para corrida)
                if "SALDO ANTERIOR" in (desc or "").upper():
                    if sal:
                        saldo_inicial = _to_float_ar(sal)
                    else:
                        nums = AMOUNT_RE.findall(" ".join(row))
                        if nums:
                            saldo_inicial = _to_float_ar(nums[-1])
                    continue

                # Filas válidas inician con fecha dd/mm/aa
                if not fecha or not FECHA_RE.match(fecha):
                    continue

                desc_limpia = (desc or "").strip()   # COMBTE se descarta
                d = _to_float_ar(deb)
                c = _to_float_ar(cre)

                if d == 0.0 and c == 0.0:
                    nums = AMOUNT_RE.findall(" ".join(row))
                    if nums:
                        amt = _to_float_ar(nums[0])
                        tipo = _classify(desc_limpia.upper())
                        if tipo == "D": d = amt
                        else: c = amt

                movimientos.append([fecha, desc_limpia, d, c])

    df = pd.DataFrame(movimientos, columns=["Fecha", "Descripción", "Débito", "Crédito"])
    return df, saldo_inicial

def _parse_pdf_text(file_bytes: bytes) -> Tuple[pd.DataFrame, Optional[float]]:
    # Fallback textual usando pdfplumber.extract_text()
    text = ""
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for p in pdf.pages:
            text += (p.extract_text() or "") + "\n"

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    saldo_inicial = None
    for ln in lines[:250]:
        if "SALDO ANTERIOR" in ln.upper():
            nums = AMOUNT_RE.findall(ln)
            if nums:
                saldo_inicial = _to_float_ar(nums[-1])
                break

    movimientos = []
    i = 0
    while i < len(lines):
        ln = lines[i]
        if FECHA_LINE_RE.match(ln):
            partes = ln.split(" ", 1)
            fecha = partes[0]
            resto = (partes[1] if len(partes) > 1 else "").strip()
            desc = AMOUNT_RE.sub("", resto).strip()

            # concatenar líneas de descripción hasta la próxima fecha o importes
            j = i + 1
            while j < len(lines) and not FECHA_LINE_RE.match(lines[j]):
                if "PAGINA" in lines[j].upper(): break
                if AMOUNT_RE.search(lines[j]): break
                desc += (" " + lines[j]).strip()
                j += 1

            # tomar primer importe de la línea como monto del movimiento
            nums = AMOUNT_RE.findall(ln)
            deb, cred = 0.0, 0.0
            if nums:
                amt = _to_float_ar(nums[0])
                tipo = _classify(desc.upper())
                if tipo == "D": deb = amt
                else: cred = amt

            movimientos.append([fecha, desc, deb, cred])
            i = j
        else:
            i += 1

    df = pd.DataFrame(movimientos, columns=["Fecha", "Descripción", "Débito", "Crédito"])
    return df, saldo_inicial

def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    """
    Devuelve: Fecha | Descripción | Débito | Crédito | Saldo
    - Descarta columna COMBTE
    - Reconstruye 'Saldo' por corrida a partir de 'SALDO ANTERIOR'
    - Soporta PDFs grandes (por páginas)
    """
    df, saldo_inicial = _parse_pdf_structured(file_bytes)
    if df.empty:
        df, saldo_inicial = _parse_pdf_text(file_bytes)

    if df.empty:
        return pd.DataFrame(columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo"])

    df["Fecha"] = df["Fecha"].apply(_normalize_fecha)
    df["Débito"] = pd.to_numeric(df["Débito"], errors="coerce").fillna(0.0)
    df["Crédito"] = pd.to_numeric(df["Crédito"], errors="coerce").fillna(0.0)

    saldo = float(saldo_inicial or 0.0)
    saldos = []
    for d, c in zip(df["Débito"], df["Crédito"]):
        saldo = saldo - float(d) + float(c)
        saldos.append(saldo)
    df["Saldo"] = saldos

    # Orden natural por fecha (manteniendo estabilidad)
    df["_i"] = range(len(df))
    df = df.sort_values(["Fecha","_i"]).drop(columns="_i").reset_index(drop=True)
    return df
