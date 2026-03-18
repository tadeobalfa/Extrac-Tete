# parsers/nacion2.py
from __future__ import annotations

import io
import re
from typing import List, Optional, Dict

import pandas as pd
import pdfplumber


DATE_START_RE = re.compile(r"^\s*(\d{2}/\d{2})(?:\s|$)")
YEAR_ONLY_RE = re.compile(r"^\s*/(20\d{2})(?:\s|$)")
YEAR_ANY_RE = re.compile(r"/(20\d{2})")
COMPROBANTE_ONLY_RE = re.compile(r"^\s*\d{4,}\s*$")

# solo número monetario, sin el $
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


def _extract_year(block: List[str]) -> str:
    for line in block:
        s = _clean(line)

        m = YEAR_ONLY_RE.match(s)
        if m:
            return m.group(1)

        m2 = YEAR_ANY_RE.search(s)
        if m2:
            return m2.group(1)

    return "2025"


def _line_amounts(line: str) -> List[str]:
    return AMOUNT_TOKEN_RE.findall(_clean(line))


def _resolve_importe_saldo(block: List[str]) -> tuple[Optional[float], Optional[float]]:
    """
    Regla correcta para Nación 2:

    1) Si una línea /2025 trae 2 montos => primero = importe, segundo = saldo
       Ej: /2025 -872.555,00 1.653.432,31

    2) Si una línea /2025 trae 1 monto y una línea previa trae 1 monto,
       la de /2025 = importe y la previa = saldo
       Ej:
         12924 DEB.TRAN.INTERB $ 168.189,98
         /2025 -138.153,00

    3) Si una línea normal trae 2 montos => primero = importe, segundo = saldo
       Ej:
         ... $ -8.788,50 $ 193.947,13

    4) Fallback final:
       usar los dos últimos montos del bloque como importe/saldo.
    """
    year_lines = []
    other_lines = []

    for line in block:
        s = _clean(line)
        amounts = _line_amounts(s)
        if not amounts:
            continue

        rec = {"line": s, "amounts": amounts}

        if YEAR_ONLY_RE.match(s) or s.startswith("/202"):
            year_lines.append(rec)
        else:
            other_lines.append(rec)

    # Regla 1
    for rec in year_lines:
        amts = rec["amounts"]
        if len(amts) >= 2:
            return _to_amount(amts[0]), _to_amount(amts[1])

    # Regla 2
    for rec in year_lines:
        amts = rec["amounts"]
        if len(amts) == 1:
            importe = _to_amount(amts[0])

            # buscar el último monto no-year previo al year line
            year_idx = block.index(rec["line"]) if rec["line"] in block else -1
            prev_candidates = []
            for i, line in enumerate(block):
                s = _clean(line)
                if i >= year_idx:
                    break
                if YEAR_ONLY_RE.match(s) or s.startswith("/202"):
                    continue
                am2 = _line_amounts(s)
                if len(am2) == 1:
                    prev_candidates.append(am2[0])
                elif len(am2) >= 2:
                    prev_candidates.append(am2[-1])

            if prev_candidates:
                saldo = _to_amount(prev_candidates[-1])
                return importe, saldo

    # Regla 3
    for rec in other_lines:
        amts = rec["amounts"]
        if len(amts) >= 2:
            return _to_amount(amts[0]), _to_amount(amts[1])

    # Regla 4
    all_amounts: List[str] = []
    for line in block:
        all_amounts.extend(_line_amounts(line))

    if len(all_amounts) >= 2:
        return _to_amount(all_amounts[-2]), _to_amount(all_amounts[-1])

    return None, None


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

    importe, saldo_pdf = _resolve_importe_saldo(block)
    if importe is None or saldo_pdf is None:
        return None

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

    # Saldo real posterior al movimiento
    df["Saldo"] = df["Saldo_PDF"].shift(-1)

    # Último movimiento cronológico
    last_idx = len(df) - 1
    df.loc[last_idx, "Saldo"] = (
        float(df.loc[last_idx, "Saldo_PDF"])
        - float(df.loc[last_idx, "Débito"])
        + float(df.loc[last_idx, "Crédito"])
    )

    return df[["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]].copy()
