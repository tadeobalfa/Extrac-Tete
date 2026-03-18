# parsers/nacion2.py
from __future__ import annotations

import io
import re
from typing import List, Dict, Optional

import pandas as pd
import pdfplumber


DATE_FULL_RE = re.compile(r"\b(\d{2}/\d{2}/\d{2,4})\b")
DATE_DDMM_RE = re.compile(r"^\s*(\d{2}/\d{2})\s*$")
YEAR_LINE_RE = re.compile(r"^\s*/?(20\d{2})\b")

AMOUNT_RE = re.compile(
    r"\$?\s*-?\s*(?:\d{1,3}(?:[.\s]\d{3})+|\d+)(?:,\d{2})-?"
)

COMPROBANTE_RE = re.compile(r"^\d{4,}$")


def _coerce_amount(s: str) -> float:
    if s is None:
        return 0.0
    s = str(s).strip()
    s = s.replace("\u2212", "-").replace("−", "-")
    s = s.replace("$", "").replace(" ", "")

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


def _clean_text(s: str) -> str:
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_header_or_noise(line: str) -> bool:
    up = _clean_text(line).upper()

    if not up:
        return True

    noise_contains = [
        "BANCO DE LA NACION ARGENTINA",
        "BANCO NACION",
        "EXTRACTO",
        "PAGINA",
        "PÁGINA",
        "MOVIMIENTOS",
        "FECHA",
        "COMPROBANTE",
        "IMPORTE",
        "SALDO",
        "TOTAL",
        "RESUMEN",
        "CUENTA CORRIENTE",
        "CUENTA",
        "CBU",
        "CBU:",
        "NRO.",
        "NÚMERO DE CUENTA",
        "NUMERO DE CUENTA",
        "MONEDA",
        "SUCURSAL",
        "HOJA",
    ]

    if up in {"$", "/2025", "/2024", "/2023"}:
        return False

    return any(x in up for x in noise_contains)


def _extract_lines_from_pdf(raw_bytes: bytes) -> List[str]:
    lines: List[str] = []

    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page in pdf.pages:
            txt = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
            page_lines = txt.splitlines()

            for line in page_lines:
                line = line.rstrip()
                if line is None:
                    continue
                lines.append(line)

    return lines


def _merge_split_dates(lines: List[str]) -> List[str]:
    """
    Une casos tipo:
      03/07
      /2025 28912520 GRAVAMEN ...
    => 03/07/2025 28912520 GRAVAMEN ...
    """
    merged: List[str] = []
    i = 0

    while i < len(lines):
        cur = _clean_text(lines[i])

        if i + 1 < len(lines):
            nxt = _clean_text(lines[i + 1])

            m_ddmm = DATE_DDMM_RE.match(cur)
            m_year = YEAR_LINE_RE.match(nxt)

            if m_ddmm and m_year:
                ddmm = m_ddmm.group(1)
                year = m_year.group(1)
                rest = re.sub(r"^\s*/?20\d{2}\s*", "", nxt).strip()
                joined = f"{ddmm}/{year}"
                if rest:
                    joined += f" {rest}"
                merged.append(joined)
                i += 2
                continue

        merged.append(cur)
        i += 1

    return merged


def _group_blocks(lines: List[str]) -> List[str]:
    """
    Agrupa líneas en bloques de movimientos.
    Un movimiento empieza cuando aparece una fecha completa dd/mm/yyyy.
    Todo lo que siga hasta la próxima fecha se considera parte del mismo bloque.
    """
    blocks: List[str] = []
    current: List[str] = []

    for line in lines:
        line = _clean_text(line)
        if not line:
            continue

        if _is_header_or_noise(line):
            continue

        if DATE_FULL_RE.search(line):
            if current:
                blocks.append(" ".join(current).strip())
            current = [line]
        else:
            if current:
                current.append(line)

    if current:
        blocks.append(" ".join(current).strip())

    return blocks


def _parse_block(block: str) -> Optional[Dict]:
    """
    Espera algo como:
    03/07/2025 28912520 GRAVAMEN LEY 25413 S/CRED $ -130,73 $ 557.846,65
    o variantes con texto adicional.
    """
    block = _clean_text(block)
    if not block:
        return None

    m_date = DATE_FULL_RE.search(block)
    if not m_date:
        return None

    fecha_txt = m_date.group(1)
    fecha = pd.to_datetime(fecha_txt, dayfirst=True, errors="coerce")
    if pd.isna(fecha):
        return None

    amounts = AMOUNT_RE.findall(block)
    if len(amounts) < 2:
        return None

    importe_txt = amounts[-2]
    saldo_pdf_txt = amounts[-1]

    importe = _coerce_amount(importe_txt)
    saldo_pdf = _coerce_amount(saldo_pdf_txt)

    debito = abs(importe) if importe < 0 else 0.0
    credito = abs(importe) if importe > 0 else 0.0

    # quitar fecha al inicio
    tail = block[m_date.end():].strip()

    # quitar importes del final
    tail = re.sub(
        rf"{re.escape(importe_txt)}\s*{re.escape(saldo_pdf_txt)}\s*$",
        "",
        tail,
        flags=re.IGNORECASE,
    ).strip()

    # quitar comprobante si está al inicio
    tail_parts = tail.split(" ", 1)
    if tail_parts and COMPROBANTE_RE.match(tail_parts[0]):
        tail = tail_parts[1].strip() if len(tail_parts) > 1 else ""

    descripcion = _clean_text(tail)

    if not descripcion:
        return None

    return {
        "Fecha": fecha,
        "Descripción": descripcion,
        "Débito": round(debito, 2),
        "Crédito": round(credito, 2),
        "Saldo_PDF": round(saldo_pdf, 2),
    }


def parse_pdf(raw_bytes: bytes) -> pd.DataFrame:
    """
    Nación 2
    - Ignora Comprobante
    - Importe negativo => Débito
    - Importe positivo => Crédito
    - El PDF viene en orden inverso; se exporta en orden cronológico
    - El saldo del PDF es saldo ANTERIOR al movimiento
    - Saldo real por fila = saldo de la fila superior del PDF
      => en cronológico: Saldo = Saldo_PDF.shift(-1)
      y el último se calcula como saldo_pdf - débito + crédito
    - Une correctamente movimientos partidos por corte de página
    """
    raw_lines = _extract_lines_from_pdf(raw_bytes)
    merged_lines = _merge_split_dates(raw_lines)
    blocks = _group_blocks(merged_lines)

    rows: List[Dict] = []
    for block in blocks:
        rec = _parse_block(block)
        if rec:
            rows.append(rec)

    if not rows:
        return pd.DataFrame(columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo"])

    df = pd.DataFrame(rows)

    # El PDF viene del más nuevo al más viejo -> pasar a cronológico
    df = df.iloc[::-1].reset_index(drop=True)

    # Saldo real posterior al movimiento:
    # saldo de la fila siguiente en cronológico (= fila superior en el PDF)
    df["Saldo"] = df["Saldo_PDF"].shift(-1)

    # último movimiento cronológico: calcular saldo final
    last_idx = len(df) - 1
    df.loc[last_idx, "Saldo"] = (
        float(df.loc[last_idx, "Saldo_PDF"])
        - float(df.loc[last_idx, "Débito"])
        + float(df.loc[last_idx, "Crédito"])
    )

    df = df[["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]].copy()

    return df