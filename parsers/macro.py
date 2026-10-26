# parsers/macro.py
# Banco Macro - "Resumen General" (una o varias cuentas en el mismo PDF).
#
# Estructura:
#   CUENTA CORRIENTE ESPECIAL EN DOLARES NRO.: 2-325-0946481994-6
#   DETALLE DE MOVIMIENTO
#   FECHA DESCRIPCION REFERENCIA DEBITOS CREDITOS SALDO
#   SALDO ULTIMO EXTRACTO AL 30/12/2024 6.891,25
#   13/01/25 CCERR LOPTRANS SR 30712057501 CIRC.CERRADO 59563783 20.000,00 26.891,25
#   SALDO FINAL AL DIA 31/01/2025 26.891,25
#
# - Cada cuenta va a su propia hoja del Excel (columna "Cuenta").
# - El encabezado de la cuenta se repite en cada hoja del PDF: se sigue
#   acumulando en la misma cuenta.
# - Débito/crédito por variación de saldo, controlado contra el importe
#   de la línea; si no coinciden se marca [REVISAR].
import io
import re
from typing import Dict, List, Optional

import pandas as pd
import pdfplumber

ACC_RE = re.compile(r"^(?P<tipo>(?:CUENTA|CAJA)\b.*?)\s+NRO\.?:\s*(?P<num>[\d\-]+)", re.IGNORECASE)
DATE_RE = re.compile(r"^(\d{2}/\d{2}/\d{2,4})\b")
MONEY_RE = re.compile(r"-?\d{1,3}(?:\.\d{3})*,\d{2}-?")
SALDO_INI_RE = re.compile(r"^SALDO ULTIMO EXTRACTO", re.IGNORECASE)
SALDO_FIN_RE = re.compile(r"^SALDO FINAL", re.IGNORECASE)


def to_number(s: str) -> float:
    t = (s or "").strip().replace("$", "")
    neg = t.startswith("-") or t.endswith("-")
    t = t.strip("-").replace(".", "").replace(",", ".")
    try:
        v = float(t)
    except ValueError:
        return 0.0
    return -v if neg else v


def _fmt_ar(v: float) -> str:
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def account_label(tipo: str, numero: str) -> str:
    """'CUENTA CORRIENTE ESPECIAL EN DOLARES' + '2-325-0946481994-6' -> 'CC ESP DOLARES 946'."""
    t = re.sub(r"\s+", " ", tipo.upper()).strip()
    usd = "DOLAR" in t
    if t.startswith("CAJA"):
        base = "CA"
    elif "ESPECIAL" in t:
        base = "CC ESP"
    else:
        base = "CC"
    digits = re.sub(r"\D", "", numero)
    return f"{base} {'DOLARES' if usd else 'PESOS'} {digits[-3:]}"[:31]


def _clean_desc(text: str) -> str:
    t = MONEY_RE.sub("", text)
    t = re.sub(r"\s{2,}", " ", t).strip()
    t = re.sub(r"\s+0$", "", t)          # referencia vacía que el banco imprime como "0"
    return t


def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    rows: List[list] = []
    order: List[str] = []
    info: Dict[str, dict] = {}       # cuenta -> {"fecha_ini", "saldo_ini", "saldo_fin"}
    prev_saldo: Dict[str, Optional[float]] = {}
    current: Optional[str] = None
    in_detail = False

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            for raw in (page.extract_text() or "").split("\n"):
                line = re.sub(r"\s{2,}", " ", raw).strip()
                if not line:
                    continue
                up = line.upper()

                m_acc = ACC_RE.match(line)
                if m_acc:
                    current = account_label(m_acc.group("tipo"), m_acc.group("num"))
                    if current not in info:
                        order.append(current)
                        info[current] = {}
                        prev_saldo[current] = None
                    in_detail = False
                    continue

                if current is None:
                    continue

                if up.startswith("DETALLE DE MOVIMIENTO"):
                    in_detail = True
                    continue
                if not in_detail or up.startswith("FECHA "):
                    continue

                if SALDO_INI_RE.match(up):
                    nums = MONEY_RE.findall(line)
                    if nums:
                        prev_saldo[current] = to_number(nums[-1])
                        info[current]["saldo_ini"] = prev_saldo[current]
                        md = re.search(r"(\d{2}/\d{2}/\d{4})", line)
                        info[current]["fecha_ini"] = md.group(1) if md else None
                    continue

                if SALDO_FIN_RE.match(up):
                    nums = MONEY_RE.findall(line)
                    if nums:
                        info[current]["saldo_fin"] = to_number(nums[-1])
                    in_detail = False
                    continue

                m = DATE_RE.match(line)
                if not m:
                    continue      # leyendas, pie de página, etc.

                nums = MONEY_RE.findall(line[m.end():])
                if not nums:
                    continue
                saldo = to_number(nums[-1])
                importe = to_number(nums[-2]) if len(nums) >= 2 else None

                base = prev_saldo[current] if prev_saldo[current] is not None else saldo
                delta = round(saldo - base, 2)
                deb, cred = max(-delta, 0.0), max(delta, 0.0)

                desc = _clean_desc(line[m.end():])
                if importe is not None and abs(abs(delta) - abs(importe)) > 0.01:
                    desc = f"{desc} [REVISAR: importe en PDF {_fmt_ar(abs(importe))}]"

                fecha = m.group(1)
                if len(fecha) == 8:
                    fecha = fecha[:6] + "20" + fecha[6:]
                rows.append([fecha, desc, round(deb, 2), round(cred, 2), round(saldo, 2), current])
                prev_saldo[current] = saldo

    cols = ["Fecha", "Descripción", "Débito", "Crédito", "Saldo", "Cuenta"]

    # Cuentas sin movimientos: una fila informativa para que aparezca su hoja
    with_movs = {r[5] for r in rows}
    for cta in order:
        if cta not in with_movs and "saldo_ini" in info[cta]:
            saldo = info[cta].get("saldo_fin", info[cta]["saldo_ini"])
            rows.append([info[cta].get("fecha_ini"), "Sin movimientos en el período", 0.0, 0.0, round(saldo, 2), cta])

    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df
    df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%Y", errors="coerce")
    pos = {c: i for i, c in enumerate(order)}
    df["_o"] = df["Cuenta"].map(pos)
    df["_i"] = range(len(df))
    return df.sort_values(["_o", "_i"]).drop(columns=["_o", "_i"]).reset_index(drop=True)[cols]
