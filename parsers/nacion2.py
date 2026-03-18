# parsers/nacion2.py
from __future__ import annotations

import io
import re
from typing import List, Optional, Dict

import pandas as pd
import pdfplumber


DATE_LINE_RE = re.compile(r"^\s*(\d{2}/\d{2})(?:\s|$)")
YEAR_ONLY_RE = re.compile(r"^\s*/(20\d{2})\s*$")
COMPROBANTE_ONLY_RE = re.compile(r"^\s*\d{4,}\s*$")

# número monetario argentino, con o sin $
AMOUNT_RE = re.compile(
    r"-?\$?\s*(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d{2})-?"
)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").replace("\xa0", " ")).strip()


def _to_amount(s: str) -> float:
    s = _clean(s)
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

    noise = [
        "ULTIMOS MOVIMIENTOS",
        "ÚLTIMOS MOVIMIENTOS",
        "FECHA COMPROBANTE CONCEPTO IMPORTE SALDO",
        "FECHA COMPROBANTE",
        "CONCEPTO IMPORTE SALDO",
    ]
    return any(x in up for x in noise)


def _build_blocks(lines: List[str]) -> List[List[str]]:
    """
    Cada movimiento empieza cuando aparece una línea que arranca con dd/mm.
    Todo lo siguiente hasta la próxima fecha pertenece al mismo movimiento.
    Esto une naturalmente cortes de página como:
      03/12
      /2025 12922 DB CREDIN TRANSFERENCIA
      $
      -114.864,00
      $ 283.053,98
    """
    blocks: List[List[str]] = []
    current: List[str] = []

    for raw in lines:
        line = raw.rstrip("\n")
        s = line.strip()

        if not s:
            continue
        if _is_header_or_noise(s):
            continue

        if DATE_LINE_RE.match(s):
            if current:
                blocks.append(current)
            current = [s]
        else:
            if current:
                current.append(s)

    if current:
        blocks.append(current)

    return blocks


def _parse_block(block: List[str]) -> Optional[Dict]:
    if not block:
        return None

    first = _clean(block[0])
    m_date = DATE_LINE_RE.match(first)
    if not m_date:
        return None

    ddmm = m_date.group(1)

    # año: buscar línea /2025 dentro del bloque
    year = None
    for ln in block:
        m_year = YEAR_ONLY_RE.match(_clean(ln))
        if m_year:
            year = m_year.group(1)
            break
    if year is None:
        # fallback por si viniera en alguna misma línea
        joined_year = re.search(r"/(20\d{2})", " ".join(block))
        year = joined_year.group(1) if joined_year else "2025"

    fecha = pd.to_datetime(f"{ddmm}/{year}", format="%d/%m/%Y", errors="coerce")
    if pd.isna(fecha):
        return None

    # Unimos TODO el bloque para tomar los importes reales aunque estén partidos
    joined = " ".join(_clean(x) for x in block if _clean(x))
    joined = _clean(joined)

    amount_tokens = AMOUNT_RE.findall(joined)

    # Todo movimiento válido debe tener sí o sí:
    # penúltimo importe = movimiento
    # último importe = saldo PDF
    if len(amount_tokens) < 2:
        return None

    importe_txt = amount_tokens[-2]
    saldo_pdf_txt = amount_tokens[-1]

    importe = _to_amount(importe_txt)
    saldo_pdf = _to_amount(saldo_pdf_txt)

    debito = abs(importe) if importe < 0 else 0.0
    credito = abs(importe) if importe > 0 else 0.0

    # ---------------------------
    # Construcción de descripción
    # ---------------------------
    desc_parts: List[str] = []

    for i, raw_line in enumerate(block):
        line = _clean(raw_line)
        if not line:
            continue

        # quitar fecha al inicio de la primera línea
        if i == 0:
            line = re.sub(r"^\d{2}/\d{2}\s*", "", line).strip()

        # quitar línea solo año
        if YEAR_ONLY_RE.match(line):
            continue

        # quitar comprobante si aparece solo
        if COMPROBANTE_ONLY_RE.match(line):
            continue

        # quitar comprobante al inicio de la línea
        line = re.sub(r"^\d{4,}\s+", "", line).strip()

        # eliminar símbolos $ sueltos
        line = line.replace("$", " ")
        line = _clean(line)

        if line:
            desc_parts.append(line)

    descripcion = " ".join(desc_parts)
    descripcion = _clean(descripcion)

    # sacar los dos últimos importes de la descripción
    # importante: solo los últimos dos, no tocar texto del medio
    for amt in [importe_txt, saldo_pdf_txt]:
        pos = descripcion.rfind(amt)
        if pos != -1:
            descripcion = _clean((descripcion[:pos] + " " + descripcion[pos + len(amt):]).strip())

    descripcion = descripcion.replace("$", " ")
    descripcion = _clean(descripcion)

    # quitar año colgado al principio si quedó como "/2025"
    descripcion = re.sub(r"^/20\d{2}\s*", "", descripcion).strip()

    # quitar comprobante al principio si sobrevivió
    descripcion = re.sub(r"^\d{4,}\s+", "", descripcion).strip()

    descripcion = _clean(descripcion)

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
    - El PDF viene del más nuevo al más viejo
    - El saldo del PDF es el saldo anterior al movimiento
    - El saldo real posterior al movimiento es el de la fila superior del PDF
      => en cronológico: Saldo = Saldo_PDF.shift(-1)
      => el último se calcula
    - Une correctamente cortes de página y renglones con '$' separado
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

    # El PDF viene en orden inverso (más nuevo -> más viejo)
    # Lo pasamos a cronológico
    df = df.iloc[::-1].reset_index(drop=True)

    # Saldo real posterior al movimiento
    df["Saldo"] = df["Saldo_PDF"].shift(-1)

    # Último movimiento cronológico: calcular saldo final
    last_idx = len(df) - 1
    df.loc[last_idx, "Saldo"] = (
        float(df.loc[last_idx, "Saldo_PDF"])
        - float(df.loc[last_idx, "Débito"])
        + float(df.loc[last_idx, "Crédito"])
    )

    return df[["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]].copy()
