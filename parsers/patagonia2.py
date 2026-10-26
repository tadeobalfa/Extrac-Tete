# parsers/patagonia2.py
# Banco Patagonia - formato "Movimientos de Cuenta" (descarga desde e-bank).
#
# Características del formato:
#   - Encabezado: "Cuenta: CC$ 070-709876268-000" / "Titularidad: ..."
#   - Columnas: Fecha | Descripción | Referencia | Débito | Crédito | Saldo
#   - Los movimientos vienen INVERTIDOS (del último día al primero).
#   - Cada movimiento está separado por una línea horizontal fina; la
#     descripción y la referencia pueden ocupar varios renglones dentro
#     de la misma fila.
#
# Se lee por posición: las líneas horizontales delimitan cada fila y los
# segmentos de esas líneas dan los límites de las columnas (cambian según
# la versión del PDF, por eso se toman de cada página).
import io
import re
from typing import List, Optional, Tuple

import pandas as pd
import pdfplumber

MONEY_RE = re.compile(r"^-?\d{1,3}(?:\.\d{3})*,\d{2}-?$")
DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
ACCOUNT_RE = re.compile(r"Cuenta:\s*(?P<tipo>[A-Z$]+)\s*(?P<num>[\d\-]+)", re.IGNORECASE)
FOOTER_RE = re.compile(r"^P.{0,2}gina$", re.IGNORECASE)   # "Página" / "Pgina"

COLS = ["fecha", "desc", "ref", "deb", "cred", "saldo"]


def to_number(s: str) -> float:
    t = (s or "").strip()
    if not t:
        return 0.0
    neg = t.startswith("-") or t.endswith("-")
    t = t.strip("-").replace(".", "").replace(",", ".")
    try:
        v = float(t)
    except ValueError:
        return 0.0
    return -v if neg else v


def _account_label(text: str) -> str:
    """'Cuenta: CC$ 070-709876268-000' -> 'CCTE PESOS 268'."""
    m = ACCOUNT_RE.search(text or "")
    if not m:
        return "CUENTA"
    tipo = m.group("tipo").upper()
    partes = [p for p in m.group("num").split("-") if p]
    # el número de cuenta es la parte del medio (sucursal-cuenta-sufijo)
    numero = max(partes, key=len) if partes else ""
    suf = re.sub(r"\D", "", numero)[-3:] or "000"
    usd = "U$S" in tipo or "USD" in tipo or "U$D" in tipo
    base = "CCTE" if tipo.startswith("CC") else ("CA" if tipo.startswith("CA") else tipo.replace("$", ""))
    return f"{base} {'USD' if usd else 'PESOS'} {suf}"


def _column_bounds(page) -> Optional[List[float]]:
    """Límites x de las 6 columnas, a partir de los segmentos de las líneas."""
    segs = sorted({(round(l["x0"]), round(l["x1"])) for l in page.lines
                   if abs(l["top"] - l["bottom"]) < 1 and l["x1"] - l["x0"] > 20})
    if len(segs) < 6:
        return None
    segs = segs[:6]
    return [s[0] for s in segs] + [segs[-1][1]]


def _row_separators(page) -> List[float]:
    tops = sorted({round(l["top"], 1) for l in page.lines if abs(l["top"] - l["bottom"]) < 1})
    out: List[float] = []
    for t in tops:
        if not out or t - out[-1] > 3:
            out.append(t)
    return out


def _col_of(x_center: float, bounds: List[float]) -> Optional[int]:
    for i in range(len(bounds) - 1):
        if bounds[i] - 2 <= x_center < bounds[i + 1] + 2:
            return i
    return None


def _cell_text(words) -> str:
    ws = sorted(words, key=lambda w: (round(w["top"]), w["x0"]))
    return re.sub(r"\s+", " ", " ".join(w["text"] for w in ws)).strip()


def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    rows: List[Tuple[str, str, str, float, float, float, str]] = []
    account = None
    bounds = None

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if account is None and "Cuenta:" in text:
                account = _account_label(text)

            bounds = _column_bounds(page) or bounds
            seps = _row_separators(page)
            if not bounds or not seps:
                continue

            words = page.extract_words(keep_blank_chars=False, use_text_flow=False)

            # límite inferior de la última fila: el pie "Página N" (o el final de la hoja)
            footer_top = min((w["top"] for w in words if FOOTER_RE.match(w["text"])), default=page.height - 40)
            limits = seps + [footer_top - 2]

            for top, bottom in zip(limits[:-1], limits[1:]):
                band = [w for w in words if top <= (w["top"] + w["bottom"]) / 2 < bottom]
                if not band:
                    continue

                cells = {c: [] for c in COLS}
                for w in band:
                    ci = _col_of((w["x0"] + w["x1"]) / 2, bounds)
                    if ci is not None:
                        cells[COLS[ci]].append(w)

                fecha = _cell_text(cells["fecha"])
                if not DATE_RE.match(fecha):
                    continue   # encabezado de tabla u otra fila que no es movimiento

                deb_txt = _cell_text(cells["deb"])
                cred_txt = _cell_text(cells["cred"])
                saldo_txt = _cell_text(cells["saldo"])
                if not MONEY_RE.match(saldo_txt):
                    continue

                desc = _cell_text(cells["desc"])
                ref = _cell_text(cells["ref"])
                descripcion = f"{desc} - {ref}" if ref else desc

                rows.append((
                    fecha,
                    descripcion,
                    deb_txt,
                    cred_txt,
                    to_number(deb_txt) if MONEY_RE.match(deb_txt) else 0.0,
                    to_number(cred_txt) if MONEY_RE.match(cred_txt) else 0.0,
                    to_number(saldo_txt),
                ))

    cols = ["Fecha", "Descripción", "Débito", "Crédito", "Saldo", "Cuenta"]
    if not rows:
        return pd.DataFrame(columns=cols)

    # El PDF viene del último movimiento al primero: se invierte.
    rows.reverse()

    out = []
    prev_saldo = None
    for fecha, descripcion, _d, _c, deb, cred, saldo in rows:
        # Control: saldo anterior - débito + crédito = saldo de la fila
        if prev_saldo is not None and abs(round(prev_saldo - deb + cred, 2) - saldo) > 0.01:
            descripcion = f"{descripcion} [REVISAR: no cierra con el saldo anterior]"
        prev_saldo = saldo
        out.append([fecha, descripcion, round(deb, 2), round(cred, 2), round(saldo, 2), account or "CUENTA"])

    df = pd.DataFrame(out, columns=cols)
    df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%Y", errors="coerce")
    return df
