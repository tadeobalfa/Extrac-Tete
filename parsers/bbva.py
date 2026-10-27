# parsers/bbva.py
# BBVA (Banco Francés) - "Resumen Pymes y Negocios / Cuentas y paquetes".
#
# Sección que se lee ("Movimientos en cuentas"), una por cuenta:
#   CC $ 084-331248/9 (Cta.Cte.Bancaria) - Iva-Responsable Inscripto
#   FECHA ORIGEN CONCEPTO DÉBITO CRÉDITO SALDO
#   SALDO ANTERIOR 14.210.990,32
#   30/12 D REG REC SIRCREB CORDOBA F:30/12/25 -19.698,35 14.191.291,97
#   02/01 D 587 TR.NE1848121 SAN CRISTOBAL SOC 261.452,48 14.448.438,49
#   SALDO AL 30 DE ENERO 29.451.774,01
#
# - Las fechas vienen sin año (dd/mm): el año se toma del período del
#   resumen ("SALDO AL 30 DE ENERO" + "ENERO 2026" / "información al:
#   02/02/2026"). Los movimientos de un mes posterior al cierre (p.ej.
#   30/12 en un resumen de enero) son del año anterior.
#   (Antes se tomaba el primer número de 4 cifras del PDF, que podía ser
#   el final de la tarjeta de débito: "1981".)
# - El débito viene con signo "-" y el crédito sin signo; se controla
#   contra el saldo de cada línea y se marca [REVISAR] si no cierra.
# - Solo se leen las líneas entre el encabezado de la tabla y
#   "SALDO AL ...": los cuadros de abajo (transferencias recibidas,
#   débitos automáticos, cheques, impuestos) se ignoran.
# - Cada cuenta va a su hoja (columna "Cuenta"), con el mismo nombre que
#   usaba la versión anterior: "CC $ 084-331248-9".
import io
import re
from collections import Counter
from typing import Dict, List, Optional

import pandas as pd
import pdfplumber

MESES = {
    "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4, "MAYO": 5, "JUNIO": 6, "JULIO": 7,
    "AGOSTO": 8, "SEPTIEMBRE": 9, "SETIEMBRE": 9, "OCTUBRE": 10, "NOVIEMBRE": 11, "DICIEMBRE": 12,
}

ACCOUNT_RE = re.compile(
    r"^(?P<tipo>CC|CA)\s+(?P<mon>\$|U\$S|USD)\s+(?P<num>\d{3}-\d{6}/\d)\s*\((?P<resto>[^)]*)\)",
    re.IGNORECASE,
)
HEADER_RE = re.compile(r"^FECHA\s+ORIGEN\s+CONCEPTO", re.IGNORECASE)
SALDO_ANT_RE = re.compile(r"^SALDO\s+ANTERIOR", re.IGNORECASE)
SALDO_AL_RE = re.compile(r"^SALDO\s+AL\s+(\d{1,2})\s+DE\s+([A-ZÁÉÍÓÚ]+)", re.IGNORECASE)
MOV_RE = re.compile(r"^(?P<d>\d{2})/(?P<m>\d{2})\s+(?P<rest>.*)$")
MONEY_RE = re.compile(r"(?<![\d/])-?\d{1,3}(?:\.\d{3})*,\d{2}(?![\d/])")
DATE_IN_DESC_RE = re.compile(r"\b(?:F:)?\d{2}/\d{2}/\d{2,4}\b")


def to_number(s: str) -> float:
    t = (s or "").strip()
    neg = t.startswith("-") or t.endswith("-")
    t = t.strip("-").replace(".", "").replace(",", ".")
    try:
        v = float(t)
    except ValueError:
        return 0.0
    return -v if neg else v


def _fmt_ar(v: float) -> str:
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _sheet_name(m: re.Match) -> str:
    mon = "U$S" if m.group("mon").upper() in ("U$S", "USD") else "$"
    return f"{m.group('tipo').upper()} {mon} {m.group('num').replace('/', '-')}"[:31]


def _closing_period(full_text: str):
    """(mes, año) de cierre del resumen."""
    up = full_text.upper()
    mes = None
    for line in up.split("\n"):
        mm = SALDO_AL_RE.match(line.strip())
        if mm and mm.group(2) in MESES:
            mes = MESES[mm.group(2)]
            break

    anio = None
    if mes:
        # "ENERO 2026" (cuadro de impuesto a los débitos y créditos)
        nombre = [k for k, v in MESES.items() if v == mes][0]
        mm = re.search(rf"\b{nombre}\s+(20\d{{2}})\b", up)
        if mm:
            anio = int(mm.group(1))
    if anio is None:
        # "información al: 02/02/2026" u otras fechas completas cercanas al cierre
        fechas = re.findall(r"\b(\d{2})/(\d{2})/(20\d{2})\b", up)
        if mes:
            cand = []
            for _d, mo, y in fechas:
                mo, y = int(mo), int(y)
                if mo == mes:
                    cand.append(y)
                elif mo == mes % 12 + 1:          # mes siguiente al cierre
                    cand.append(y - 1 if mes == 12 else y)
            if cand:
                anio = Counter(cand).most_common(1)[0][0]
        if anio is None and fechas:
            anio = Counter(int(y) for *_x, y in fechas).most_common(1)[0][0]
    if anio is None:
        anio = pd.Timestamp.today().year
    return mes, anio


def _clean_desc(rest: str) -> str:
    t = MONEY_RE.sub(" ", rest)
    t = DATE_IN_DESC_RE.sub(" ", t)
    return re.sub(r"\s{2,}", " ", t).strip(" -")


def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        pages = [(p.extract_text() or "") for p in pdf.pages]

    mes_cierre, anio_cierre = _closing_period("\n".join(pages))

    def fecha_de(d: str, m: str) -> Optional[pd.Timestamp]:
        dd, mm = int(d), int(m)
        if mm < 1 or mm > 12 or dd < 1:
            return None
        y = anio_cierre - 1 if (mes_cierre and mm > mes_cierre) else anio_cierre
        try:
            return pd.Timestamp(year=y, month=mm, day=dd)
        except ValueError:
            return None

    rows: List[list] = []
    order: List[str] = []
    info: Dict[str, dict] = {}
    current: Optional[str] = None
    in_table = False
    prev_saldo: Optional[float] = None

    for text in pages:
        in_table = False   # cada página: esperar el encabezado de la tabla
        for raw in text.split("\n"):
            line = re.sub(r"\s{2,}", " ", raw).strip()
            if not line:
                continue

            m_acc = ACCOUNT_RE.match(line)
            if m_acc:
                cta = _sheet_name(m_acc)
                if cta != current:
                    current = cta
                    prev_saldo = info.get(cta, {}).get("ultimo")
                if cta not in info:
                    info[cta] = {}
                    order.append(cta)
                in_table = False
                continue

            if HEADER_RE.match(line):
                in_table = current is not None
                continue

            if not in_table or current is None:
                continue

            if SALDO_ANT_RE.match(line):
                nums = MONEY_RE.findall(line)
                if nums:
                    prev_saldo = to_number(nums[-1])
                    info[current]["saldo_ant"] = prev_saldo
                continue

            m_fin = SALDO_AL_RE.match(line)
            if m_fin:
                nums = MONEY_RE.findall(line)
                if nums:
                    info[current]["saldo_fin"] = to_number(nums[-1])
                info[current]["dia_cierre"] = int(m_fin.group(1))
                in_table = False          # termina la tabla de esta cuenta
                continue

            if line.upper().startswith("TOTAL MOVIMIENTOS"):
                in_table = False
                continue

            m = MOV_RE.match(line)
            if m:
                rest = m.group("rest")
                if "SIN MOVIMIENTOS" in rest.upper():
                    continue
                fecha = fecha_de(m.group("d"), m.group("m"))
                nums = MONEY_RE.findall(rest)
                if fecha is None or not nums:
                    continue

                saldo = to_number(nums[-1])
                importe = to_number(nums[-2]) if len(nums) >= 2 else None
                base = prev_saldo if prev_saldo is not None else (
                    saldo - importe if importe is not None else saldo)
                delta = round(saldo - base, 2)

                if importe is not None:
                    deb, cred = (abs(importe), 0.0) if importe < 0 else (0.0, importe)
                else:
                    deb, cred = max(-delta, 0.0), max(delta, 0.0)

                desc = _clean_desc(rest)
                if abs(round(cred - deb, 2) - delta) > 0.01:
                    desc = f"{desc} [REVISAR: el saldo varía {_fmt_ar(delta)}]"

                rows.append([fecha, desc, round(deb, 2), round(cred, 2), round(saldo, 2), current])
                prev_saldo = saldo
                info[current]["ultimo"] = saldo
                continue

            # renglón extra de descripción (sin fecha ni importes)
            if rows and rows[-1][5] == current and not MONEY_RE.search(line):
                rows[-1][1] = f"{rows[-1][1]} / {line}"

    # Cuentas sin movimientos: una fila informativa para que aparezca su hoja
    con_movs = {r[5] for r in rows}
    for cta in order:
        if cta in con_movs or "saldo_ant" not in info[cta]:
            continue
        dia = info[cta].get("dia_cierre")
        try:
            fecha = pd.Timestamp(year=anio_cierre, month=mes_cierre, day=dia) if (mes_cierre and dia) else pd.NaT
        except ValueError:
            fecha = pd.NaT
        saldo = info[cta].get("saldo_fin", info[cta]["saldo_ant"])
        rows.append([fecha, "Sin movimientos en el período", 0.0, 0.0, round(saldo, 2), cta])

    cols = ["Fecha", "Descripción", "Débito", "Crédito", "Saldo", "Cuenta"]
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df
    pos = {c: i for i, c in enumerate(order)}
    df["_o"] = df["Cuenta"].map(pos)
    df["_i"] = range(len(df))
    return df.sort_values(["_o", "_i"]).drop(columns=["_o", "_i"]).reset_index(drop=True)[cols]
