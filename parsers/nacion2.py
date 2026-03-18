# parsers/nacion2.py
from __future__ import annotations

import io
import re
from typing import List, Optional, Dict

import pandas as pd
import pdfplumber


DATE_START_RE = re.compile(r"^\s*(\d{2}/\d{2})(?:\s|$)")
YEAR_INLINE_RE = re.compile(r"/(20\d{2})")
YEAR_ONLY_RE = re.compile(r"^\s*/(20\d{2})\s*$")
COMPROBANTE_ONLY_RE = re.compile(r"^\s*\d{4,}\s*$")

# monto argentino con signo, con o sin $
AMOUNT_TOKEN_RE = re.compile(r"-?\d{1,3}(?:\.\d{3})*,\d{2}")


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").replace("\xa0", " ")).strip()


def _to_amount(num_txt: str) -> float:
    s = _clean(num_txt).replace("\u2212", "-").replace("−", "-").replace("$", "").replace(" ", "")
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


def _extract_lines(raw_bytes: bytes) -> List[str]:
    lines: List[str] = []

    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            for line in txt.splitlines():
                line = line.rstrip()
                if line is not None:
                    lines.append(line)

    return lines


def _is_header_or_noise(line: str) -> bool:
    up = _clean(line).upper()
    if not up:
        return True

    bad = [
        "ULTIMOS MOVIMIENTOS",
        "ÚLTIMOS MOVIMIENTOS",
        "FECHA COMPROBANTE CONCEPTO IMPORTE SALDO",
        "FECHA COMPROBANTE",
        "CONCEPTO IMPORTE SALDO",
        "FECHA:",
    ]
    return any(x in up for x in bad)


def _build_blocks(lines: List[str]) -> List[List[str]]:
    """
    Cada movimiento empieza cuando aparece una línea que arranca con dd/mm.
    Todo lo siguiente hasta la próxima fecha pertenece al mismo movimiento.
    Esto resuelve automáticamente cortes de página y casos tipo:
      03/12
      /2025 12922 DB CREDIN TRANSFERENCIA
      $
      -114.864,00
      $ 283.053,98
    """
    blocks: List[List[str]] = []
    current: List[str] = []

    for raw in lines:
        s = raw.strip()
        if not s:
            continue
        if _is_header_or_noise(s):
            continue

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


def _extract_amounts_in_order(block: List[str]) -> List[str]:
    """
    Devuelve todos los importes del bloque en el orden visual/textual en que aparecen.
    Ignora los '$' sueltos.
    """
    nums: List[str] = []
    for line in block:
        line = _clean(line)
        found = AMOUNT_TOKEN_RE.findall(line)
        if found:
            nums.extend(found)
    return nums


def _extract_year(block: List[str]) -> str:
    for line in block:
        s = _clean(line)
        m = YEAR_ONLY_RE.match(s)
        if m:
            return m.group(1)

        m2 = YEAR_INLINE_RE.search(s)
        if m2:
            return m2.group(1)

    return "2025"


def _strip_line_for_description(line: str, is_first_line: bool) -> str:
    s = _clean(line)

    if not s:
        return ""

    if is_first_line:
        s = re.sub(r"^\d{2}/\d{2}\s*", "", s).strip()

    s = re.sub(r"^/20\d{2}\s*", "", s).strip()
    s = re.sub(r"^\d{4,}\s+", "", s).strip()

    if YEAR_ONLY_RE.match(s):
        return ""
    if COMPROBANTE_ONLY_RE.match(s):
        return ""

    # sacar importes y símbolos de dinero de la descripción
    s = s.replace("$", " ")
    s = AMOUNT_TOKEN_RE.sub(" ", s)
    s = _clean(s)

    return s


def _parse_block(block: List[str]) -> Optional[Dict]:
    if not block:
        return None

    first = _clean(block[0])
    m_date = DATE_START_RE.match(first)
    if not m_date:
        return None

    ddmm = m_date.group(1)
    year = _extract_year(block)

    fecha = pd.to_datetime(f"{ddmm}/{year}", format="%d/%m/%Y", errors="coerce")
    if pd.isna(fecha):
        return None

    amount_tokens = _extract_amounts_in_order(block)

    # Regla clave: todo movimiento válido debe tener sí o sí:
    # penúltimo monto = importe del movimiento
    # último monto = saldo PDF
    if len(amount_tokens) < 2:
        return None

    importe_txt = amount_tokens[-2]
    saldo_pdf_txt = amount_tokens[-1]

    importe = _to_amount(importe_txt)
    saldo_pdf = _to_amount(saldo_pdf_txt)

    debito = abs(importe) if importe < 0 else 0.0
    credito = abs(importe) if importe > 0 else 0.0

    desc_parts: List[str] = []
    for i, line in enumerate(block):
        part = _strip_line_for_description(line, is_first_line=(i == 0))
        if part:
            desc_parts.append(part)

    descripcion = _clean(" ".join(desc_parts))
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
    - No toma Comprobante
    - Importe negativo => Débito
    - Importe positivo => Crédito
    - Orden del PDF invertido => salida cronológica
    - Corta movimientos por bloques iniciados en dd/mm
    - Soporta /2025 en línea aparte
    - Soporta '$' separado del número
    - Soporta cortes de página
    - Saldo del PDF = saldo anterior al movimiento
    - Saldo real de salida = saldo de la fila superior del PDF
    """
    lines = _extract_lines(raw_bytes)
    blocks = _build_blocks(lines)

    rows: List[Dict] = []
    for block in blocks:
        rec = _parse_block(block)
        if rec is not None:
            rows.append(rec)

    if not rows:
        return pd.DataFrame(columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo"])

    df = pd.DataFrame(rows)

    # PDF: más nuevo -> más viejo
    # salida: cronológico
    df = df.iloc[::-1].reset_index(drop=True)

    # saldo real posterior al movimiento
    df["Saldo"] = df["Saldo_PDF"].shift(-1)

    # último movimiento cronológico
    last_idx = len(df) - 1
    df.loc[last_idx, "Saldo"] = (
        float(df.loc[last_idx, "Saldo_PDF"])
        - float(df.loc[last_idx, "Débito"])
        + float(df.loc[last_idx, "Crédito"])
    )

    return df[["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]].copy()
