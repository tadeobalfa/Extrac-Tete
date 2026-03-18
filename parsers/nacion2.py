from __future__ import annotations

import io
import re
from typing import List, Dict, Optional

import pandas as pd
import pdfplumber


DATE_DDMM_RE = re.compile(r"^\d{2}/\d{2}$")
YEAR_RE = re.compile(r"^/?(20\d{2})$")
COMPROBANTE_RE = re.compile(r"^\d{4,}$")
AMOUNT_RE = re.compile(r"^-?(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d{2})-?$")


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").replace("\xa0", " ")).strip()


def _to_amount(s: str) -> Optional[float]:
    s = _clean(s).replace("$", "").replace(" ", "").replace("\u2212", "-").replace("−", "-")
    if not s:
        return None

    neg = s.startswith("-") or s.endswith("-")
    s = s.lstrip("-")
    if s.endswith("-"):
        s = s[:-1]

    s = s.replace(".", "").replace(",", ".")

    try:
        v = float(s)
    except Exception:
        return None

    return -v if neg else v


def _group_rows(words: List[dict], y_tol: float = 2.5) -> List[Dict]:
    rows: List[Dict] = []

    for w in sorted(words, key=lambda z: (float(z["top"]), float(z["x0"]))):
        y = float(w["top"])

        placed = False
        for row in rows:
            if abs(row["y"] - y) <= y_tol:
                row["words"].append(w)
                row["y"] = (row["y"] * row["n"] + y) / (row["n"] + 1)
                row["n"] += 1
                placed = True
                break

        if not placed:
            rows.append({"y": y, "n": 1, "words": [w]})

    for row in rows:
        row["words"] = sorted(row["words"], key=lambda z: float(z["x0"]))

    return rows


def _find_cuts(rows: List[Dict]):
    """
    Detecta los cortes de columnas tomando la fila de encabezados:
    Fecha | Comprobante | Concepto | Importe | Saldo
    """
    for row in rows:
        toks = [_clean(w["text"]) for w in row["words"]]
        up = " ".join(t.upper() for t in toks)

        if "FECHA" in up and "COMPROBANTE" in up and "IMPORTE" in up and "SALDO" in up:
            cuts = {}

            for w in row["words"]:
                t = _clean(w["text"]).upper()
                x0 = float(w["x0"])

                if t.startswith("FECHA"):
                    cuts["fecha"] = x0
                elif t.startswith("COMPROBANTE"):
                    cuts["comp"] = x0
                elif t.startswith("CONCEPTO"):
                    cuts["concepto"] = x0
                elif t.startswith("IMPORTE"):
                    cuts["imp"] = x0
                elif t.startswith("SALDO"):
                    cuts["saldo"] = x0

            if {"comp", "concepto", "imp", "saldo"} <= set(cuts.keys()):
                return cuts, row["y"]

    return None, None


def _split_cols(row: Dict, cuts: Dict) -> Dict[str, List[str]]:
    cols = {"date": [], "comp": [], "desc": [], "imp": [], "saldo": []}

    for w in row["words"]:
        x = float(w["x0"])
        t = _clean(w["text"])

        if x < cuts["comp"]:
            cols["date"].append(t)
        elif x < cuts["concepto"]:
            cols["comp"].append(t)
        elif x < cuts["imp"]:
            cols["desc"].append(t)
        elif x < cuts["saldo"]:
            cols["imp"].append(t)
        else:
            cols["saldo"].append(t)

    return cols


def parse_pdf(raw_bytes: bytes) -> pd.DataFrame:
    """
    Nación 2

    Reglas:
    - No usa Comprobante
    - Importe negativo => Débito
    - Importe positivo => Crédito
    - El PDF viene del más nuevo al más viejo
    - El saldo del PDF es el saldo ANTERIOR al movimiento
    - El saldo real posterior del movimiento es el de la fila superior del PDF
    - Soporta:
        * /2025 en línea aparte
        * $ separado del número
        * descripción multilínea
        * cortes de página
    """
    current = None
    recs: List[Dict] = []

    cuts = None
    header_y = 140.0

    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            words = page.extract_words(
                keep_blank_chars=False,
                use_text_flow=False,
                x_tolerance=1,
                y_tolerance=1,
            ) or []

            if not words:
                continue

            rows = _group_rows(words, y_tol=2.5)

            new_cuts, new_header_y = _find_cuts(rows)
            if new_cuts is not None:
                cuts = new_cuts
                header_y = new_header_y + 5

            if cuts is None:
                continue

            for row in rows:
                if page_idx == 0 and row["y"] <= header_y:
                    continue

                cols = _split_cols(row, cuts)

                date_toks = [t for t in cols["date"] if t and t != "$"]
                comp_toks = [t for t in cols["comp"] if t and t != "$"]
                desc_toks = [t for t in cols["desc"] if t and t != "$"]
                imp_toks = [t for t in cols["imp"] if t and t != "$"]
                saldo_toks = [t for t in cols["saldo"] if t and t != "$"]

                row_up = " ".join(date_toks + comp_toks + desc_toks + imp_toks + saldo_toks).upper()

                if not row_up:
                    continue

                if "ULTIMOS MOVIMIENTOS" in row_up:
                    continue

                if "FECHA" in row_up and "COMPROBANTE" in row_up and "IMPORTE" in row_up and "SALDO" in row_up:
                    continue

                ddmm = next((t for t in date_toks if DATE_DDMM_RE.match(t)), None)

                if ddmm is not None:
                    if current is not None:
                        recs.append(current)

                    current = {
                        "ddmm": ddmm,
                        "year": None,
                        "desc": [],
                        "imp": None,
                        "saldo_pdf": None,
                    }

                    yy = next((YEAR_RE.match(t).group(1) for t in date_toks if YEAR_RE.match(t)), None)
                    if yy:
                        current["year"] = yy

                    if comp_toks and not COMPROBANTE_RE.match(comp_toks[0]):
                        desc_toks = comp_toks + desc_toks

                    current["desc"].extend(desc_toks)

                    imp_nums = [_to_amount(t) for t in imp_toks if AMOUNT_RE.match(t)]
                    imp_nums = [x for x in imp_nums if x is not None]
                    if imp_nums:
                        current["imp"] = imp_nums[-1]

                    saldo_nums = [_to_amount(t) for t in saldo_toks if AMOUNT_RE.match(t)]
                    saldo_nums = [x for x in saldo_nums if x is not None]
                    if saldo_nums:
                        current["saldo_pdf"] = saldo_nums[-1]

                    continue

                if current is None:
                    continue

                yy = next((YEAR_RE.match(t).group(1) for t in date_toks if YEAR_RE.match(t)), None)
                if yy and current["year"] is None:
                    current["year"] = yy

                if comp_toks:
                    if COMPROBANTE_RE.match(comp_toks[0]):
                        extra_desc = comp_toks[1:]
                    else:
                        extra_desc = comp_toks
                    current["desc"].extend(extra_desc)

                current["desc"].extend(desc_toks)

                imp_nums = [_to_amount(t) for t in imp_toks if AMOUNT_RE.match(t)]
                imp_nums = [x for x in imp_nums if x is not None]
                if imp_nums:
                    current["imp"] = imp_nums[-1]

                saldo_nums = [_to_amount(t) for t in saldo_toks if AMOUNT_RE.match(t)]
                saldo_nums = [x for x in saldo_nums if x is not None]
                if saldo_nums:
                    current["saldo_pdf"] = saldo_nums[-1]

    if current is not None:
        recs.append(current)

    rows_out: List[Dict] = []

    for r in recs:
        desc = _clean(" ".join(r["desc"]))

        if not r["ddmm"] or not r["year"] or not desc:
            continue
        if r["imp"] is None or r["saldo_pdf"] is None:
            continue

        fecha = pd.to_datetime(f"{r['ddmm']}/{r['year']}", format="%d/%m/%Y", errors="coerce")
        if pd.isna(fecha):
            continue

        imp = float(r["imp"])
        saldo_pdf = float(r["saldo_pdf"])

        rows_out.append(
            {
                "Fecha": fecha,
                "Descripción": desc,
                "Débito": round(abs(imp) if imp < 0 else 0.0, 2),
                "Crédito": round(abs(imp) if imp > 0 else 0.0, 2),
                "Saldo_PDF": round(saldo_pdf, 2),
            }
        )

    if not rows_out:
        return pd.DataFrame(columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo"])

    df = pd.DataFrame(rows_out)

    # PDF: más nuevo -> más viejo
    # salida: cronológico
    df = df.iloc[::-1].reset_index(drop=True)

    # Nación 2: el saldo del PDF es el saldo ANTERIOR al movimiento
    # El saldo real posterior de cada movimiento es el de la fila superior del PDF
    df["Saldo"] = df["Saldo_PDF"].shift(-1)

    last_idx = len(df) - 1
    df.loc[last_idx, "Saldo"] = (
        float(df.loc[last_idx, "Saldo_PDF"])
        - float(df.loc[last_idx, "Débito"])
        + float(df.loc[last_idx, "Crédito"])
    )

    return df[["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]].copy()
