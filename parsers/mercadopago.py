# parsers/mercadopago.py
import io
import re
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd
import pdfplumber

# === Reglas/regex portadas de tu mp_to_excel.py ===
DATE_RE   = re.compile(r"^\d{2}-\d{2}-\d{4}$")
MONEY_RE  = re.compile(r"^\$?\s*-?\d{1,3}(?:\.\d{3})*,\d{2}-?$")
ID_RE     = re.compile(r"^\d{8,14}$")
PAGE_NO   = re.compile(r"^\d{1,3}/\d{1,3}$")
HEADER_TOKENS = {"fecha","descripción","descripcion","valor","saldo","id","de","la","operación","operacion"}

def _parse_money(s: str) -> Optional[float]:
    s = s.replace("\xa0"," ").replace("\u2009"," ").replace("\u202f"," ")
    s = s.replace("$","").replace(" ","").strip()
    neg_trailing = s.endswith("-")
    if neg_trailing:
        s = s[:-1]
    s = s.replace(".","").replace(",",".")
    try:
        v = float(s)
        return -v if neg_trailing else v
    except:
        return None

def _to_date(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%d-%m-%Y")
    except:
        return None

def _find_header_and_columns(page) -> Tuple[float, Dict[str, Tuple[float,float]]]:
    """
    Detecta encabezado (Fecha/Descripción/Valor/Saldo) y define rangos X por columna.
    """
    words = page.extract_words(x_tolerance=2.0, y_tolerance=3.0, keep_blank_chars=False, use_text_flow=True)
    lines = {}
    for w in words:
        y = round(w["top"], 1)
        lines.setdefault(y, []).append(w)

    header_y = None
    cols = None

    for y, toks in lines.items():
        toks_sorted = sorted(toks, key=lambda w: w["x0"])
        txs = [t["text"].lower() for t in toks_sorted]
        if ("fecha" in txs) and ("valor" in txs) and ("saldo" in txs) and any(t in txs for t in ["descripción","descripcion"]):
            header_y = y
            xs = {}
            for t in toks_sorted:
                txt = t["text"].lower()
                if txt == "fecha": xs["fecha"] = t["x0"]
                if txt in ("descripción","descripcion"): xs["desc"] = t["x0"]
                if txt == "valor": xs["valor"] = t["x0"]
                if txt == "saldo": xs["saldo"] = t["x0"]
                if txt == "id": xs["id"] = t["x0"]

            if set(["fecha","desc","valor","saldo"]).issubset(xs):
                cut_fd = (xs["fecha"] + xs["desc"]) / 2
                right_desc_anchor = xs.get("id", xs["valor"])
                cut_di = (xs["desc"] + right_desc_anchor) / 2
                cut_iv = (right_desc_anchor + xs["valor"]) / 2 if "id" in xs else xs["valor"]
                cut_vs = (xs["valor"] + xs["saldo"]) / 2
                cols = {
                    "date_r": (0, cut_fd),
                    "desc_r": (cut_fd, cut_di),
                    "val_r":  (cut_iv, cut_vs),
                    "sal_r":  (cut_vs, page.width),
                }
            break

    # Fallback por si no encuentra títulos en la página
    if cols is None:
        W = page.width
        header_y = 0.0
        cols = {
            "date_r": (0, W*0.15),
            "desc_r": (W*0.15, W*0.62),
            "val_r":  (W*0.70, W*0.84),
            "sal_r":  (W*0.84, W),
        }
    return header_y, cols

def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    """
    Mercado Pago → DataFrame unificado:
    Respeta orden EXACTO del PDF; Descripción = 1ª línea; Valor→Débito/Crédito; Saldo textual.
    """
    records: List[Dict] = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            header_y, cols = _find_header_and_columns(page)
            words = page.extract_words(x_tolerance=2.2, y_tolerance=3.2, keep_blank_chars=False, use_text_flow=True)

            # Fechas debajo del header, orden natural de arriba hacia abajo
            dates = [w for w in words
                     if w["top"] > header_y + 0.5
                        and cols["date_r"][0] <= w["x0"] < cols["date_r"][1]
                        and DATE_RE.fullmatch(w["text"].strip())]
            dates = sorted(dates, key=lambda w: w["top"])

            for i, d in enumerate(dates):
                # Banda del movimiento (desde un poco arriba de la fecha hasta la fecha siguiente)
                y0 = max(header_y + 1.0, d["top"] - 12.0)
                y1 = dates[i+1]["top"] - 0.3 if i+1 < len(dates) else page.height
                band_words = [w for w in words if y0 <= w["top"] < y1]

                # --- Descripción = SOLO la primera línea visual en su columna ---
                desc_tokens_all = [w for w in band_words
                    if cols["desc_r"][0] <= w["x0"] < cols["desc_r"][1]
                       and not (("$" in w["text"])
                                or MONEY_RE.fullmatch(w["text"].strip())
                                or ID_RE.fullmatch(w["text"].strip())
                                or (w["text"].strip().lower() in HEADER_TOKENS)
                                or PAGE_NO.fullmatch(w["text"].strip())
                                or w["text"].strip().lower().startswith("cvu"))]

                if desc_tokens_all:
                    min_y = min(w["top"] for w in desc_tokens_all)
                    first_line = [w for w in desc_tokens_all if abs(w["top"] - min_y) <= 1.2]
                    first_line_sorted = sorted(first_line, key=lambda w: w["x0"])
                    descripcion = " ".join([t["text"].strip() for t in first_line_sorted]).strip()
                else:
                    descripcion = ""

                # --- Valores ---
                val_tokens = [w for w in band_words
                              if cols["val_r"][0] <= w["x0"] < cols["val_r"][1]
                              and MONEY_RE.fullmatch(w["text"].strip())]
                sal_tokens = [w for w in band_words
                              if cols["sal_r"][0] <= w["x0"] < cols["sal_r"][1]
                              and MONEY_RE.fullmatch(w["text"].strip())]

                valor = _parse_money(val_tokens[-1]["text"]) if val_tokens else None
                saldo = _parse_money(sal_tokens[-1]["text"]) if sal_tokens else None

                # Respaldo: tomar los dos importes más a la derecha del bloque si falta alguno
                if valor is None or saldo is None:
                    monies_any = [(w["x0"], w["text"]) for w in band_words if MONEY_RE.fullmatch(w["text"].strip())]
                    monies_any.sort(key=lambda t: t[0])
                    if len(monies_any) >= 2:
                        saldo = _parse_money(monies_any[-1][1])
                        valor = _parse_money(monies_any[-2][1])

                debito  = abs(valor) if (valor is not None and valor < 0) else 0.0
                credito = valor if (valor is not None and valor > 0) else 0.0

                records.append({
                    "Fecha":       _to_date(d["text"].strip()),
                    "Descripción": descripcion,
                    "Débito":      debito,
                    "Crédito":     credito,
                    "Saldo":       saldo if saldo is not None else np.nan,
                })

    df = pd.DataFrame(records, columns=["Fecha","Descripción","Débito","Crédito","Saldo"])
    # ¡No reordenar! El orden ya es el del PDF.
    return df
