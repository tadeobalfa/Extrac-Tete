import io
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pdfplumber

MAX_PAGES = int(os.getenv("EXTRACTOS_MAX_PAGES", "0"))
MAX_LINES = int(os.getenv("EXTRACTOS_MAX_LINES", "60000"))

PERIOD_RE       = re.compile(r"PERIODO\s+(\d{2})-(\d{2})-(\d{4})\s+AL\s+(\d{2})-(\d{2})-(\d{4})", re.IGNORECASE)
SALDO_INI_RE    = re.compile(r"SALDO\s+ULTIMO\s+EXTRACTO\s+AL", re.IGNORECASE)
MONEY_TOKEN_RE  = re.compile(r"^[−-]?\$?\s?\d{1,3}(?:\.\d{3})*,\d{2}-?$")
DATE_DM_RE      = re.compile(r"^\d{2}-\d{2}$")

def to_float_ar(tok: str) -> float:
    if not tok:
        return 0.0
    t = tok.strip().replace("−","-").replace("$","")
    neg = False
    if t.endswith("-"):
        neg = True
        t = t[:-1]
    if t.startswith("-"):
        neg = True
        t = t[1:]
    t = t.replace(".", "").replace(",", ".")
    try:
        v = float(t)
    except Exception:
        v = 0.0
    return -v if neg else v

def normalize_date_dm(dm: str, year: int) -> str:
    d, m = dm.split("-")
    return f"{d}/{m}/{year:04d}"

def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    rows: List[Tuple[pd.Timestamp, str, float, float]] = []
    year: Optional[int] = None
    saldo_inicial: Optional[float] = None

    page_count = 0
    line_count = 0

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            page_count += 1
            if MAX_PAGES and page_count > MAX_PAGES:
                break
            text = page.extract_text() or ""
            mper = PERIOD_RE.search(text)
            if mper and year is None:
                year = int(mper.group(3))
            for ln in text.split("\n"):
                line_count += 1
                if line_count > MAX_LINES:
                    break
                if SALDO_INI_RE.search(ln):
                    nums = re.findall(r"\d{1,3}(?:\.\d{3})*,\d{2}-?", ln)
                    if nums:
                        saldo_inicial = to_float_ar(nums[-1])
            if line_count > MAX_LINES:
                break

        page_count2 = 0
        line_count2 = 0
        for page in pdf.pages:
            page_count2 += 1
            if MAX_PAGES and page_count2 > MAX_PAGES:
                break

            words = page.extract_words(use_text_flow=False, keep_blank_chars=False)
            if not words:
                continue

            x_deb = x_cred = x_saldo = None
            for w in words:
                t = w["text"].strip().upper()
                cx = (w["x0"] + w["x1"]) / 2.0
                if x_deb is None and t in ("DEBITOS", "DÉBITOS", "DEBITO", "DÉBITO"):
                    x_deb = cx
                elif x_cred is None and t in ("CREDITOS", "CRÉDITOS", "CREDITO", "CRÉDITO"):
                    x_cred = cx
                elif x_saldo is None and t.startswith("SALDO"):
                    x_saldo = cx

            if not (x_deb and x_cred and x_saldo):
                continue

            cut1 = (x_deb + x_cred) / 2.0
            cut2 = (x_cred + x_saldo) / 2.0
            if cut2 <= cut1:
                cut2 = page.width * 0.95

            rows_y: Dict[float, List[dict]] = {}
            for w in words:
                ykey = round(w["top"] / 2) * 2
                rows_y.setdefault(ykey, []).append(w)

            for y, ws in sorted(rows_y.items(), key=lambda kv: kv[0]):
                line_count2 += 1
                if line_count2 > MAX_LINES:
                    break

                ws_sorted = sorted(ws, key=lambda ww: ww["x0"])
                if not ws_sorted:
                    continue
                first = ws_sorted[0]["text"].strip()
                if not DATE_DM_RE.match(first):
                    continue

                date_dm = first
                desc_parts: List[str] = []
                deb_val = 0.0
                cred_val = 0.0

                for w in ws_sorted[1:]:
                    t = w["text"].strip()
                    cx = (w["x0"] + w["x1"]) / 2.0

                    if MONEY_TOKEN_RE.match(t):
                        if cx < cut1:
                            deb_val += abs(to_float_ar(t))
                        elif cx < cut2:
                            cred_val += abs(to_float_ar(t))
                        else:
                            pass
                    else:
                        if cx < cut1:
                            desc_parts.append(t)

                descr = re.sub(r"\s{2,}", " ", " ".join(desc_parts)).strip()
                if year is None:
                    year = datetime.now().year

                rows.append((
                    pd.to_datetime(normalize_date_dm(date_dm, year), format="%d/%m/%Y", errors="coerce"),
                    descr,
                    deb_val,
                    cred_val
                ))

            if line_count2 > MAX_LINES:
                break

    df = pd.DataFrame(rows, columns=["Fecha", "Descripción completa", "Débito", "Crédito"])
    if df.empty:
        return pd.DataFrame(columns=["Fecha", "Descripción completa", "Débito", "Crédito", "Saldo"])

    df["_i"] = range(len(df))
    df = df.sort_values(["Fecha", "_i"]).drop(columns="_i").reset_index(drop=True)

    saldo_ini_val = saldo_inicial if saldo_inicial is not None else 0.0
    saldo = saldo_ini_val
    saldos = []
    for d, c in zip(df["Débito"], df["Crédito"]):
        saldo = saldo - d + c
        saldos.append(saldo)
    df["Saldo"] = saldos

    # SIN fila artificial "SALDO INICIAL"
    return df[["Fecha", "Descripción completa", "Débito", "Crédito", "Saldo"]]