# parsers/nacion2.py
from __future__ import annotations

import io
import re
from dataclasses import dataclass
from typing import List, Optional, Dict

import pandas as pd
import pdfplumber


DATE_DDMM_RE = re.compile(r"^\d{2}/\d{2}$")
YEAR_RE = re.compile(r"^/?(20\d{2})$")
COMPROBANTE_RE = re.compile(r"^\d{4,}$")
AMOUNT_RE = re.compile(r"^-?(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d{2})-?$")


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


@dataclass
class MovimientoParcial:
    ddmm: str
    year: Optional[str]
    comprobante: str
    descripcion: List[str]
    importe: Optional[float]
    saldo_pdf: Optional[float]


def _group_rows(words: List[dict], y_tol: float = 3.5) -> List[List[dict]]:
    rows: List[List[dict]] = []

    for w in sorted(words, key=lambda z: (float(z["top"]), float(z["x0"]))):
        y = float(w["top"])
        placed = False

        for row in rows:
            row_y = sum(float(a["top"]) for a in row) / len(row)
            if abs(row_y - y) <= y_tol:
                row.append(w)
                placed = True
                break

        if not placed:
            rows.append([w])

    for row in rows:
        row.sort(key=lambda z: float(z["x0"]))

    return rows


def _row_tokens(row: List[dict]):
    out = []
    for w in row:
        out.append({
            "text": _clean(w["text"]),
            "x0": float(w["x0"]),
            "x1": float(w["x1"]),
            "top": float(w["top"]),
        })
    return out


def _find_header_cutoffs(rows: List[List[dict]]):
    """
    Busca la fila de encabezado y toma cortes de columnas.
    """
    for row in rows:
        toks = _row_tokens(row)
        txt = " ".join(t["text"].upper() for t in toks)

        if "FECHA" in txt and "COMPROBANTE" in txt and "IMPORTE" in txt and "SALDO" in txt:
            x_comp = None
            x_imp = None
            x_sal = None

            for t in toks:
                up = t["text"].upper()
                if up.startswith("COMPROBANTE"):
                    x_comp = t["x0"]
                elif up.startswith("IMPORTE"):
                    x_imp = t["x0"]
                elif up.startswith("SALDO"):
                    x_sal = t["x0"]

            if x_comp is not None and x_imp is not None and x_sal is not None:
                return x_comp, x_imp, x_sal

    return None


def _classify_token(text: str) -> bool:
    return bool(AMOUNT_RE.match(text))


def _parse_page(page) -> List[MovimientoParcial]:
    words = page.extract_words(
        keep_blank_chars=False,
        use_text_flow=False,
        x_tolerance=1,
        y_tolerance=1,
    ) or []

    if not words:
        return []

    rows = _group_rows(words)
    cutoffs = _find_header_cutoffs(rows)
    if not cutoffs:
        return []

    x_comp, x_imp, x_sal = cutoffs

    movimientos: List[MovimientoParcial] = []
    cur: Optional[MovimientoParcial] = None

    for row in rows:
        toks = _row_tokens(row)
        if not toks:
            continue

        row_text = " ".join(t["text"] for t in toks).upper()

        # saltar encabezados y ruido obvio
        if (
            "FECHA" in row_text and "COMPROBANTE" in row_text and "IMPORTE" in row_text and "SALDO" in row_text
        ) or "ULTIMOS MOVIMIENTOS" in row_text or "ÚLTIMOS MOVIMIENTOS" in row_text:
            continue

        # dividir por columnas
        left = [t for t in toks if t["x0"] < x_comp]
        mid = [t for t in toks if x_comp <= t["x0"] < x_imp]
        imp = [t for t in toks if x_imp <= t["x0"] < x_sal]
        sal = [t for t in toks if t["x0"] >= x_sal]

        left_txts = [t["text"] for t in left if t["text"]]
        mid_txts = [t["text"] for t in mid if t["text"]]
        imp_txts = [t["text"] for t in imp if t["text"] not in {"$"}]
        sal_txts = [t["text"] for t in sal if t["text"] not in {"$"}]

        # ¿arranca movimiento nuevo?
        ddmm = None
        year = None

        for tx in left_txts:
            if DATE_DDMM_RE.match(tx):
                ddmm = tx
                break

        for tx in left_txts:
            m_year = YEAR_RE.match(tx)
            if m_year:
                year = m_year.group(1)
                break

        if ddmm is not None:
            if cur is not None:
                movimientos.append(cur)

            comprobante = ""
            descripcion = []

            if mid_txts:
                if COMPROBANTE_RE.match(mid_txts[0]):
                    comprobante = mid_txts[0]
                    descripcion = mid_txts[1:]
                else:
                    descripcion = mid_txts[:]

            importe_val = None
            saldo_val = None

            imp_nums = [x for x in imp_txts if _classify_token(x)]
            sal_nums = [x for x in sal_txts if _classify_token(x)]

            if imp_nums:
                importe_val = _to_amount(imp_nums[-1])
            if sal_nums:
                saldo_val = _to_amount(sal_nums[-1])

            cur = MovimientoParcial(
                ddmm=ddmm,
                year=year,
                comprobante=comprobante,
                descripcion=descripcion,
                importe=importe_val,
                saldo_pdf=saldo_val,
            )
            continue

        # continuación del movimiento actual
        if cur is None:
            continue

        # completar año si viene en línea aparte
        if cur.year is None:
            for tx in left_txts:
                m_year = YEAR_RE.match(tx)
                if m_year:
                    cur.year = m_year.group(1)
                    break

        # completar comprobante si vino abajo
        if not cur.comprobante and mid_txts:
            if COMPROBANTE_RE.match(mid_txts[0]):
                cur.comprobante = mid_txts[0]
                extra_desc = mid_txts[1:]
            else:
                extra_desc = mid_txts
        else:
            extra_desc = mid_txts

        if extra_desc:
            cur.descripcion.extend(extra_desc)

        # importe
        imp_nums = [x for x in imp_txts if _classify_token(x)]
        if imp_nums:
            cur.importe = _to_amount(imp_nums[-1])

        # saldo
        sal_nums = [x for x in sal_txts if _classify_token(x)]
        if sal_nums:
            cur.saldo_pdf = _to_amount(sal_nums[-1])

        # caso especial: cuando el importe cae debajo del año y sigue estando
        # en la columna IMPORTE aunque visualmente parezca otra línea
        # o cuando saldo queda todavía en la columna IMPORTE por desfasaje mínimo
        if cur.importe is None and cur.saldo_pdf is None:
            all_nums = [t["text"] for t in toks if _classify_token(t["text"])]
            if len(all_nums) >= 2:
                cur.importe = _to_amount(all_nums[-2])
                cur.saldo_pdf = _to_amount(all_nums[-1])
            elif len(all_nums) == 1:
                # si ya tengo importe, esto probablemente sea saldo; si no, importe
                v = _to_amount(all_nums[0])
                if cur.importe is None:
                    cur.importe = v
                else:
                    cur.saldo_pdf = v

    if cur is not None:
        movimientos.append(cur)

    return movimientos


def _merge_cross_page(movs: List[MovimientoParcial]) -> List[MovimientoParcial]:
    """
    Une movimientos cortados entre páginas:
    - puede venir fecha sola al final de página
    - y el resto al inicio de la siguiente
    """
    merged: List[MovimientoParcial] = []

    for m in movs:
        if (
            merged
            and merged[-1].year is None
            and not merged[-1].descripcion
            and merged[-1].importe is None
            and merged[-1].saldo_pdf is None
        ):
            prev = merged.pop()

            ddmm = prev.ddmm
            year = m.year
            comprobante = m.comprobante
            descripcion = m.descripcion[:]
            importe = m.importe
            saldo_pdf = m.saldo_pdf

            merged.append(
                MovimientoParcial(
                    ddmm=ddmm,
                    year=year,
                    comprobante=comprobante,
                    descripcion=descripcion,
                    importe=importe,
                    saldo_pdf=saldo_pdf,
                )
            )
        else:
            merged.append(m)

    return merged


def parse_pdf(raw_bytes: bytes) -> pd.DataFrame:
    movimientos: List[MovimientoParcial] = []

    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page in pdf.pages:
            movimientos.extend(_parse_page(page))

    movimientos = _merge_cross_page(movimientos)

    rows: List[Dict] = []
    for m in movimientos:
        if not m.ddmm or not m.year:
            continue
        if m.importe is None or m.saldo_pdf is None:
            continue

        fecha = pd.to_datetime(f"{m.ddmm}/{m.year}", format="%d/%m/%Y", errors="coerce")
        if pd.isna(fecha):
            continue

        descripcion = _clean(" ".join(m.descripcion))
        if not descripcion:
            continue

        importe = float(m.importe)
        saldo_pdf = float(m.saldo_pdf)

        debito = abs(importe) if importe < 0 else 0.0
        credito = abs(importe) if importe > 0 else 0.0

        rows.append(
            {
                "Fecha": fecha,
                "Descripción": descripcion,
                "Débito": round(debito, 2),
                "Crédito": round(credito, 2),
                "Saldo_PDF": round(saldo_pdf, 2),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo"])

    df = pd.DataFrame(rows)

    # PDF: más nuevo -> más viejo
    # salida: cronológico
    df = df.iloc[::-1].reset_index(drop=True)

    # Nación 2: el saldo del PDF es el saldo ANTERIOR al movimiento
    # El saldo real posterior es el de la fila superior del PDF
    # => en cronológico: shift(-1)
    df["Saldo"] = df["Saldo_PDF"].shift(-1)

    last_idx = len(df) - 1
    df.loc[last_idx, "Saldo"] = (
        float(df.loc[last_idx, "Saldo_PDF"])
        - float(df.loc[last_idx, "Débito"])
        + float(df.loc[last_idx, "Crédito"])
    )

    return df[["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]].copy()
