# parsers/nacion2.py
from __future__ import annotations

import io
import re
from typing import List, Optional

import pandas as pd
import pdfplumber


DATE_START_RE = re.compile(r"^(\d{2}/\d{2})(?:\s|$)")
YEAR_RE = re.compile(r"/((?:20)\d{2})")
AMOUNT_RE = re.compile(r"-?\$?\s*(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d{2})-?")


def _to_amount(s: str) -> float:
    s = str(s or "").strip()
    s = s.replace("\u2212", "-").replace("−", "-")
    s = s.replace("$", "").replace(" ", "")

    if not s:
        return 0.0

    neg = s.startswith("-") or s.endswith("-")
    s = s.strip("-")
    s = s.replace(".", "").replace(",", ".")

    try:
        v = float(s)
    except Exception:
        return 0.0

    return -v if neg else v


def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").replace("\xa0", " ")).strip()


def _extract_lines(raw_bytes: bytes) -> List[str]:
    lines: List[str] = []

    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            for line in txt.splitlines():
                line = line.rstrip()
                if line:
                    lines.append(line)

    return lines


def _build_blocks(lines: List[str]) -> List[List[str]]:
    """
    Cada movimiento empieza cuando aparece una línea que arranca con dd/mm.
    Todo lo siguiente hasta la próxima fecha pertenece al mismo movimiento.
    Esto ya resuelve automáticamente los cortes de página del tipo:
      03/07
      /2025 ...
    """
    blocks: List[List[str]] = []
    current: List[str] = []

    for line in lines:
        s = line.strip()

        if DATE_START_RE.match(s):
            if current:
                blocks.append(current)
            current = [s]
        else:
            if current:
                current.append(s)

    if current:
        blocks.append(current)

    return blocks


def _parse_block(block_lines: List[str]) -> Optional[dict]:
    full = " | ".join(block_lines)
    full = _clean_spaces(full)

    m_date = DATE_START_RE.match(block_lines[0].strip())
    if not m_date:
        return None

    ddmm = m_date.group(1)

    m_year = YEAR_RE.search(full)
    year = m_year.group(1) if m_year else "2025"

    fecha = pd.to_datetime(f"{ddmm}/{year}", format="%d/%m/%Y", errors="coerce")
    if pd.isna(fecha):
        return None

    amounts = AMOUNT_RE.findall(full)
    if len(amounts) < 2:
        return None

    importe_txt = amounts[-2]
    saldo_pdf_txt = amounts[-1]

    importe = _to_amount(importe_txt)
    saldo_pdf = _to_amount(saldo_pdf_txt)

    debito = abs(importe) if importe < 0 else 0.0
    credito = abs(importe) if importe > 0 else 0.0

    desc_parts: List[str] = []

    # -------------------------
    # Línea 1
    # -------------------------
    line1 = block_lines[0].strip()
    line1 = line1[len(ddmm):].strip()
    line1 = AMOUNT_RE.sub(" ", line1)
    line1 = line1.replace("$", " ")
    line1 = re.sub(r"^\d{4,}\b", "", line1).strip()
    line1 = _clean_spaces(line1)
    if line1:
        desc_parts.append(line1)

    # -------------------------
    # Líneas intermedias
    # -------------------------
    for mid in block_lines[1:-1]:
        s = mid.strip()
        s = re.sub(r"^\d{4,}\b", "", s).strip()   # quitar comprobante
        s = AMOUNT_RE.sub(" ", s)
        s = s.replace("$", " ")
        s = _clean_spaces(s)
        if s:
            desc_parts.append(s)

    # -------------------------
    # Última línea
    # -------------------------
    if len(block_lines) >= 2:
        last = block_lines[-1].strip()
        last = re.sub(r"^/20\d{2}\b", "", last).strip()  # quitar /2025
        last = AMOUNT_RE.sub(" ", last)                  # quitar importe/saldo si vinieron acá
        last = last.replace("$", " ")
        last = _clean_spaces(last)
        if last:
            desc_parts.append(last)

    descripcion = _clean_spaces(" ".join(desc_parts))

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
    Lógica EXACTA validada con el Excel bueno:
    - no usa Comprobante
    - importe negativo = débito
    - importe positivo = crédito
    - el PDF viene en orden inverso
    - el saldo del PDF es el saldo ANTERIOR al movimiento
    - el saldo real del movimiento es el de la fila superior del PDF
      => en orden cronológico: Saldo = Saldo_PDF.shift(-1)
      => el último se calcula
    """
    lines = _extract_lines(raw_bytes)
    blocks = _build_blocks(lines)

    rows = []
    for block in blocks:
        rec = _parse_block(block)
        if rec is not None:
            rows.append(rec)

    if not rows:
        return pd.DataFrame(columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo"])

    df = pd.DataFrame(rows)

    # El PDF viene del más nuevo al más viejo -> invertir a cronológico
    df = df.iloc[::-1].reset_index(drop=True)

    # Saldo real posterior al movimiento
    df["Saldo"] = df["Saldo_PDF"].shift(-1)

    # Último movimiento cronológico: calcular saldo final
    i = len(df) - 1
    df.loc[i, "Saldo"] = (
        float(df.loc[i, "Saldo_PDF"])
        - float(df.loc[i, "Débito"])
        + float(df.loc[i, "Crédito"])
    )

    return df[["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]].copy()
