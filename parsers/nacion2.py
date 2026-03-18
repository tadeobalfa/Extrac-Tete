# parsers/nacion2.py
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


# Cortes visuales fijos del layout Nación 2
X_DATE_END = 82.16
X_COMP_END = 178.16
X_CONCEPT_END = 414.86
X_IMPORTE_END = 501.41
X_SALDO_END = 594.00


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").replace("\xa0", " ")).strip()


def _to_amount(s: str) -> float:
    s = _clean(s).replace("\u2212", "-").replace("−", "-").replace("$", "").replace(" ", "")
    if not s:
        return 0.0

    neg = s.startswith("-") or s.endswith("-")
    s = s.lstrip("-")
    if s.endswith("-"):
        s = s[:-1]

    s = s.replace(".", "").replace(",", ".")
    try:
        v = float(s)
    except Exception:
        return 0.0
    return -v if neg else v


def _group_words_by_band(words: List[dict], top_y: float, bottom_y: float) -> List[dict]:
    out = []
    for w in words:
        wt = float(w["top"])
        if top_y <= wt < bottom_y:
            out.append(
                {
                    "text": _clean(w["text"]),
                    "x0": float(w["x0"]),
                    "x1": float(w["x1"]),
                    "top": wt,
                }
            )
    out.sort(key=lambda z: (z["top"], z["x0"]))
    return out


def _find_separator_tops(page) -> List[float]:
    """
    Toma las líneas grises horizontales finitas (rectángulos muy bajos) como separadores reales de movimientos.
    """
    tops = []

    for r in page.rects:
        height = float(r.get("height", 0) or 0)
        width = float(r.get("width", 0) or 0)
        top = float(r.get("top", 0) or 0)
        x0 = float(r.get("x0", 0) or 0)
        x1 = float(r.get("x1", 0) or 0)

        # separadores horizontales finitos de la tabla
        if height <= 1.2 and width > 40 and 15 <= x0 <= 20 and 500 <= x1 <= 595:
            tops.append(round(top, 2))

    # únicos ordenados
    tops = sorted(set(tops))
    return tops


def _find_header_bottom(page) -> float:
    """
    Busca el encabezado 'Fecha Comprobante Concepto Importe Saldo'
    y devuelve una cota desde la cual empiezan los movimientos.
    """
    words = page.extract_words(
        keep_blank_chars=False,
        use_text_flow=False,
        x_tolerance=1,
        y_tolerance=1,
    ) or []

    header_tops = []
    for w in words:
        txt = _clean(w["text"]).upper()
        if txt in {"FECHA", "COMPROBANTE", "CONCEPTO", "IMPORTE", "SALDO"}:
            header_tops.append(float(w["top"]))

    if header_tops:
        return max(header_tops) + 20

    return 140.0


def _extract_band_record(words_band: List[dict]) -> Optional[Dict]:
    """
    Procesa un bloque visual completo entre dos líneas grises.
    Dentro del bloque puede haber:
      - fecha en una línea y /2025 abajo
      - comprobante en otra línea
      - concepto multilínea
      - '$' solo en una línea y número abajo
    """
    if not words_band:
        return None

    date_tokens = []
    comp_tokens = []
    concept_tokens = []
    importe_tokens = []
    saldo_tokens = []

    for w in words_band:
        txt = w["text"]
        x0 = w["x0"]

        if x0 < X_DATE_END:
            date_tokens.append(txt)
        elif x0 < X_COMP_END:
            comp_tokens.append(txt)
        elif x0 < X_CONCEPT_END:
            concept_tokens.append(txt)
        elif x0 < X_IMPORTE_END:
            if txt != "$":
                importe_tokens.append(txt)
        else:
            if txt != "$":
                saldo_tokens.append(txt)

    # fecha
    ddmm = None
    year = None

    for t in date_tokens:
        if DATE_DDMM_RE.match(t):
            ddmm = t
            break

    for t in date_tokens:
        m = YEAR_RE.match(t)
        if m:
            year = m.group(1)
            break

    # comprobante
    comprobante = ""
    for t in comp_tokens:
        if COMPROBANTE_RE.match(t):
            comprobante = t
            break

    # descripción
    descripcion_parts = []
    for t in concept_tokens:
        if t and t != "$":
            descripcion_parts.append(t)
    descripcion = _clean(" ".join(descripcion_parts))

    # importes
    importe_num = None
    saldo_num = None

    imp_nums = [t for t in importe_tokens if AMOUNT_RE.match(t)]
    sal_nums = [t for t in saldo_tokens if AMOUNT_RE.match(t)]

    if imp_nums:
        importe_num = _to_amount(imp_nums[-1])
    if sal_nums:
        saldo_num = _to_amount(sal_nums[-1])

    # fallback extremo: si por alguna razón el importe quedó dentro del bloque pero fuera de columna
    all_nums = [w["text"] for w in words_band if AMOUNT_RE.match(w["text"])]
    if importe_num is None and saldo_num is None and len(all_nums) >= 2:
        importe_num = _to_amount(all_nums[-2])
        saldo_num = _to_amount(all_nums[-1])

    if ddmm is None:
        return {
            "Fecha_DDMM": None,
            "Year": year,
            "Comprobante": comprobante,
            "Descripción": descripcion,
            "Débito": None,
            "Crédito": None,
            "Saldo_PDF": saldo_num,
            "Importe": importe_num,
        }

    if not descripcion:
        return {
            "Fecha_DDMM": ddmm,
            "Year": year,
            "Comprobante": comprobante,
            "Descripción": "",
            "Débito": None,
            "Crédito": None,
            "Saldo_PDF": saldo_num,
            "Importe": importe_num,
        }

    if importe_num is None or saldo_num is None:
        return {
            "Fecha_DDMM": ddmm,
            "Year": year,
            "Comprobante": comprobante,
            "Descripción": descripcion,
            "Débito": None,
            "Crédito": None,
            "Saldo_PDF": saldo_num,
            "Importe": importe_num,
        }

    debito = abs(importe_num) if importe_num < 0 else 0.0
    credito = abs(importe_num) if importe_num > 0 else 0.0

    return {
        "Fecha_DDMM": ddmm,
        "Year": year,
        "Comprobante": comprobante,
        "Descripción": descripcion,
        "Débito": round(debito, 2),
        "Crédito": round(credito, 2),
        "Saldo_PDF": round(saldo_num, 2),
        "Importe": round(importe_num, 2),
    }


def _extract_page_records(page) -> List[Dict]:
    words = page.extract_words(
        keep_blank_chars=False,
        use_text_flow=False,
        x_tolerance=1,
        y_tolerance=1,
    ) or []

    if not words:
        return []

    sep_tops = _find_separator_tops(page)
    header_bottom = _find_header_bottom(page)

    # nos quedamos con separadores por debajo del header
    sep_tops = [y for y in sep_tops if y > header_bottom]

    if not sep_tops:
        return []

    records = []

    prev_top = header_bottom
    for sep_top in sep_tops:
        band_words = _group_words_by_band(words, prev_top, sep_top)
        rec = _extract_band_record(band_words)
        if rec is not None:
            records.append(rec)
        prev_top = sep_top

    return records


def _merge_cross_page(records: List[Dict]) -> List[Dict]:
    """
    Une casos donde al final de página queda solo la fecha dd/mm
    y en la página siguiente viene /2025 + resto del movimiento.
    """
    merged = []

    i = 0
    while i < len(records):
        cur = records[i]

        incomplete_date_only = (
            cur.get("Fecha_DDMM")
            and not cur.get("Year")
            and not cur.get("Descripción")
            and cur.get("Importe") is None
            and cur.get("Saldo_PDF") is None
        )

        if incomplete_date_only and i + 1 < len(records):
            nxt = records[i + 1]
            new_rec = dict(nxt)
            new_rec["Fecha_DDMM"] = cur["Fecha_DDMM"]
            merged.append(new_rec)
            i += 2
            continue

        merged.append(cur)
        i += 1

    return merged


def parse_pdf(raw_bytes: bytes) -> pd.DataFrame:
    all_records: List[Dict] = []

    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page in pdf.pages:
            all_records.extend(_extract_page_records(page))

    all_records = _merge_cross_page(all_records)

    rows = []
    for r in all_records:
        ddmm = r.get("Fecha_DDMM")
        year = r.get("Year")
        desc = _clean(r.get("Descripción", ""))
        importe = r.get("Importe")
        saldo_pdf = r.get("Saldo_PDF")

        # todo movimiento válido debe tener sí o sí fecha, descripción, importe y saldo
        if not ddmm or not year or not desc:
            continue
        if importe is None or saldo_pdf is None:
            continue

        fecha = pd.to_datetime(f"{ddmm}/{year}", format="%d/%m/%Y", errors="coerce")
        if pd.isna(fecha):
            continue

        debito = abs(float(importe)) if float(importe) < 0 else 0.0
        credito = abs(float(importe)) if float(importe) > 0 else 0.0

        rows.append(
            {
                "Fecha": fecha,
                "Descripción": desc,
                "Débito": round(debito, 2),
                "Crédito": round(credito, 2),
                "Saldo_PDF": round(float(saldo_pdf), 2),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo"])

    df = pd.DataFrame(rows)

    # PDF viene del más nuevo al más viejo
    df = df.iloc[::-1].reset_index(drop=True)

    # Nación 2:
    # el saldo del PDF es el saldo ANTERIOR al movimiento
    # el saldo real posterior es el saldo de la fila superior del PDF
    df["Saldo"] = df["Saldo_PDF"].shift(-1)

    # último movimiento cronológico
    last_idx = len(df) - 1
    df.loc[last_idx, "Saldo"] = (
        float(df.loc[last_idx, "Saldo_PDF"])
        - float(df.loc[last_idx, "Débito"])
        + float(df.loc[last_idx, "Crédito"])
    )

    return df[["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]].copy()
