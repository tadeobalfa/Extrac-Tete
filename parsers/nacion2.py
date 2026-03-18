# parsers/nacion2.py
from __future__ import annotations

import io
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import pdfplumber


DATE_FULL_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
DATE_DDMM_RE = re.compile(r"^\d{2}/\d{2}$")
YEAR_RE = re.compile(r"^/?(20\d{2})$")
AMT_RE = re.compile(r"^-?\$?\s*(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d{2})-?$")


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def _is_amount(s: str) -> bool:
    s = _norm(s).replace(" ", "")
    return bool(AMT_RE.match(s))


def _to_amount(s: str) -> float:
    s = _norm(s).replace("$", "").replace(" ", "").replace("\u2212", "-").replace("−", "-")
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


def _to_date(s: str) -> pd.Timestamp:
    return pd.to_datetime(_norm(s), format="%d/%m/%Y", errors="coerce")


def _is_probable_header(text: str) -> bool:
    up = _norm(text).upper()
    return (
        "FECHA" in up
        and "COMPROBANTE" in up
        and "IMPORTE" in up
        and "SALDO" in up
    )


def _is_noise_text(text: str) -> bool:
    up = _norm(text).upper()
    if not up:
        return True

    bad_contains = [
        "BANCO DE LA NACION ARGENTINA",
        "PÁGINA",
        "PAGINA",
        "EXTRACTO",
        "RESUMEN",
        "CUENTA",
        "CBU",
        "CUIT",
        "SUCURSAL",
        "DOMICILIO",
        "MONEDA",
        "FECHA COMPROBANTE",
        "FECHA   COMPROBANTE",
        "FECHA COMPROBANTE DESCRIPCION",
        "DESCRIPCION IMPORTE SALDO",
        "DESCRIPCIÓN IMPORTE SALDO",
    ]
    return any(x in up for x in bad_contains)


@dataclass
class WordRow:
    y: float
    words: List[dict]


@dataclass
class ParsedRow:
    fecha: pd.Timestamp
    descripcion: str
    debito: float
    credito: float
    saldo_pdf: float


def _group_words_to_rows(words: List[dict], y_tol: float = 3.0) -> List[WordRow]:
    words = sorted(words, key=lambda w: (round(float(w["top"]), 1), float(w["x0"])))
    rows: List[WordRow] = []

    for w in words:
        y = float(w["top"])
        placed = False

        for row in rows:
            if abs(row.y - y) <= y_tol:
                row.words.append(w)
                placed = True
                break

        if not placed:
            rows.append(WordRow(y=y, words=[w]))

    for row in rows:
        row.words.sort(key=lambda w: float(w["x0"]))

    rows.sort(key=lambda r: r.y)
    return rows


def _row_text(row: WordRow) -> str:
    return _norm(" ".join(_norm(w["text"]) for w in row.words))


def _find_header_positions(rows: List[WordRow]) -> Optional[Tuple[float, float, float]]:
    """
    Devuelve cortes x aproximados:
    - x_desc_start
    - x_importe_start
    - x_saldo_start
    """
    for row in rows:
        txt = _row_text(row)
        if not _is_probable_header(txt):
            continue

        fecha_x = None
        comp_x = None
        imp_x = None
        saldo_x = None

        for w in row.words:
            t = _norm(w["text"]).upper()
            x0 = float(w["x0"])
            if t.startswith("FECHA"):
                fecha_x = x0
            elif t.startswith("COMPROBANTE"):
                comp_x = x0
            elif t.startswith("IMPORTE"):
                imp_x = x0
            elif t.startswith("SALDO"):
                saldo_x = x0

        if imp_x is not None and saldo_x is not None:
            desc_start = comp_x if comp_x is not None else (fecha_x + 60 if fecha_x is not None else 120)
            return desc_start, imp_x, saldo_x

    return None


def _extract_row_fields(row: WordRow, x_desc_start: float, x_importe_start: float, x_saldo_start: float):
    left = []
    middle = []
    right = []

    for w in row.words:
        x0 = float(w["x0"])
        txt = _norm(w["text"])

        if x0 < x_desc_start:
            left.append(txt)
        elif x0 < x_importe_start:
            middle.append(txt)
        elif x0 < x_saldo_start:
            right.append(("IMP", txt, x0))
        else:
            right.append(("SAL", txt, x0))

    left_txt = _norm(" ".join(left))
    middle_txt = _norm(" ".join(middle))

    imp_tokens = [t for kind, t, _x in right if kind == "IMP"]
    sal_tokens = [t for kind, t, _x in right if kind == "SAL"]

    importe_txt = _norm(" ".join(imp_tokens))
    saldo_txt = _norm(" ".join(sal_tokens))

    return left_txt, middle_txt, importe_txt, saldo_txt


def _parse_visual_row(
    left_txt: str,
    middle_txt: str,
    importe_txt: str,
    saldo_txt: str,
) -> Optional[ParsedRow]:
    """
    left_txt suele traer fecha
    middle_txt suele traer comprobante + descripción
    """
    if not left_txt and not middle_txt:
        return None

    left_parts = left_txt.split()
    if not left_parts:
        return None

    fecha_txt = left_parts[0]
    if not DATE_FULL_RE.match(fecha_txt):
        return None

    fecha = _to_date(fecha_txt)
    if pd.isna(fecha):
        return None

    desc = _norm(middle_txt)
    if not desc:
        return None

    # quitar comprobante al inicio
    desc_parts = desc.split(" ", 1)
    if desc_parts and re.fullmatch(r"\d{4,}", desc_parts[0]):
        desc = desc_parts[1].strip() if len(desc_parts) > 1 else ""

    if not desc:
        return None

    if not _is_amount(importe_txt) or not _is_amount(saldo_txt):
        return None

    importe = _to_amount(importe_txt)
    saldo_pdf = _to_amount(saldo_txt)

    deb = abs(importe) if importe < 0 else 0.0
    cred = abs(importe) if importe > 0 else 0.0

    return ParsedRow(
        fecha=fecha,
        descripcion=desc,
        debito=round(deb, 2),
        credito=round(cred, 2),
        saldo_pdf=round(saldo_pdf, 2),
    )


def _merge_page_cut_rows(prev_parts: Optional[dict], current_parts: dict) -> Tuple[Optional[dict], Optional[dict]]:
    """
    Une casos como:
      fin página:   left='03/07'   middle='' importe='' saldo=''
      pág siguiente left='/2025'   middle='28912520 ...' importe='$ -130,73' saldo='$ 557.846,65'
    """
    if not prev_parts:
        return None, current_parts

    prev_left = _norm(prev_parts.get("left", ""))
    cur_left = _norm(current_parts.get("left", ""))

    if DATE_DDMM_RE.match(prev_left) and YEAR_RE.match(cur_left):
        full_date = f"{prev_left}/{YEAR_RE.match(cur_left).group(1)}"
        merged = {
            "left": full_date,
            "middle": _norm(current_parts.get("middle", "")),
            "importe": _norm(current_parts.get("importe", "")),
            "saldo": _norm(current_parts.get("saldo", "")),
        }
        return None, merged

    return prev_parts, current_parts


def parse_pdf(raw_bytes: bytes) -> pd.DataFrame:
    rows_out: List[ParsedRow] = []
    carry_partial: Optional[dict] = None

    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(
                keep_blank_chars=False,
                use_text_flow=False,
                x_tolerance=2,
                y_tolerance=2,
            ) or []

            if not words:
                continue

            grouped = _group_words_to_rows(words)
            header_pos = _find_header_positions(grouped)

            if not header_pos:
                continue

            x_desc_start, x_importe_start, x_saldo_start = header_pos

            page_parts: List[dict] = []

            for row in grouped:
                txt = _row_text(row)
                if _is_noise_text(txt) or _is_probable_header(txt):
                    continue

                left_txt, middle_txt, importe_txt, saldo_txt = _extract_row_fields(
                    row, x_desc_start, x_importe_start, x_saldo_start
                )

                if not left_txt and not middle_txt and not importe_txt and not saldo_txt:
                    continue

                page_parts.append({
                    "left": left_txt,
                    "middle": middle_txt,
                    "importe": importe_txt,
                    "saldo": saldo_txt,
                })

            if not page_parts:
                continue

            normalized_parts: List[dict] = []

            # unir arrastre de página anterior con primera fila actual si corresponde
            first = page_parts[0]
            carry_partial, first_merged = _merge_page_cut_rows(carry_partial, first)

            if first_merged is not None:
                normalized_parts.append(first_merged)

            normalized_parts.extend(page_parts[1:])

            # detectar si la última fila quedó cortada al final de página
            if normalized_parts:
                last = normalized_parts[-1]
                last_left = _norm(last.get("left", ""))
                last_mid = _norm(last.get("middle", ""))
                last_imp = _norm(last.get("importe", ""))
                last_sal = _norm(last.get("saldo", ""))

                if DATE_DDMM_RE.match(last_left) and not last_mid and not last_imp and not last_sal:
                    carry_partial = last
                    normalized_parts = normalized_parts[:-1]
                else:
                    carry_partial = None

            for part in normalized_parts:
                parsed = _parse_visual_row(
                    left_txt=part.get("left", ""),
                    middle_txt=part.get("middle", ""),
                    importe_txt=part.get("importe", ""),
                    saldo_txt=part.get("saldo", ""),
                )
                if parsed is not None:
                    rows_out.append(parsed)

    if not rows_out:
        return pd.DataFrame(columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo"])

    # El PDF viene del más nuevo al más viejo.
    # Para exportar cronológico:
    df = pd.DataFrame([{
        "Fecha": r.fecha,
        "Descripción": r.descripcion,
        "Débito": r.debito,
        "Crédito": r.credito,
        "Saldo_PDF": r.saldo_pdf,
    } for r in rows_out])

    df = df.iloc[::-1].reset_index(drop=True)

    # Nación 2:
    # el saldo mostrado en cada fila del PDF es el saldo ANTERIOR al movimiento.
    # El saldo real posterior de cada movimiento es el saldo de la fila superior del PDF.
    # En orden cronológico => shift(-1)
    df["Saldo"] = df["Saldo_PDF"].shift(-1)

    if not df.empty:
        i = len(df) - 1
        df.loc[i, "Saldo"] = (
            float(df.loc[i, "Saldo_PDF"])
            - float(df.loc[i, "Débito"])
            + float(df.loc[i, "Crédito"])
        )

    return df[["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]].copy()
