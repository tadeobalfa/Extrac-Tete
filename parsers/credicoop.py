# parsers/credicoop.py
# Banco Credicoop - resumen de Cuenta Corriente / Caja de Ahorro.
#
# Formato (texto monoespaciado, sin líneas de tabla):
#   F E C H A  C O M B T E  D E S C R I P C I O N  D E B I T O  C R E D I T O  S A L D O
#   SALDO ANTERIOR                                                        −3.055,04
#   12/04/21 414694 Transf. Interbanking e/ cuentas propias      6.000,00
#                   Ord.:30715242989−ALIMENTOS GRAN PARQUE
#   12/04/21        Impuesto Ley 25.413 Alic Gral s/Debitos 0,08              2.931,57
#   SALDO AL 30/04/21                                                        819,31
#
# - Débito / crédito se deciden por la COLUMNA donde está el importe
#   (posición x según el encabezado), no por palabras clave.
# - El saldo solo aparece al final de cada día: se calcula la corrida desde
#   SALDO ANTERIOR y se controla contra esos saldos informados.
# - Se lee solo la tabla de movimientos (hasta "SALDO AL"); los cuadros de
#   abajo (impuestos, intereses, detalle de transferencias) se ignoran.
import io
import re
from typing import List, Optional

import pandas as pd
import pdfplumber

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{2}$")
AMOUNT_RE = re.compile(r"^[−\-]?\d{1,3}(?:\.\d{3})*,\d{2}-?$")
COMBTE_RE = re.compile(r"^\d{1,10}$")


def _to_float(txt: str) -> float:
    t = (txt or "").strip().replace("−", "-").replace("–", "-")
    neg = t.startswith("-") or t.endswith("-")
    t = t.strip("-").replace(".", "").replace(",", ".")
    try:
        v = float(t)
    except ValueError:
        return 0.0
    return -v if neg else v


def _clean(s: str) -> str:
    s = (s or "").replace("−", "-").replace("–", "-")
    return re.sub(r"\s+", " ", s).strip()


def _group_lines(words, tol: float = 2.0):
    lines = []
    for w in sorted(words, key=lambda w: (round(w["top"]), w["x0"])):
        if lines and abs(lines[-1][0] - w["top"]) <= tol:
            lines[-1][1].append(w)
        else:
            lines.append([w["top"], [w]])
    return [sorted(ws, key=lambda w: w["x0"]) for _, ws in lines]


def _header_layout(line_words) -> Optional[dict]:
    """Columnas a partir del encabezado deletreado 'F E C H A ... S A L D O'."""
    txt = "".join(w["text"] for w in line_words).upper()
    if not ("FECHA" in txt and "DEBITO" in txt and "CREDITO" in txt and "SALDO" in txt):
        return None

    def center(word: str) -> Optional[float]:
        letters = [w for w in line_words if len(w["text"]) == 1]
        s = "".join(w["text"].upper() for w in letters)
        i = s.find(word)
        if i < 0:
            return None
        ws = letters[i:i + len(word)]
        return (ws[0]["x0"] + ws[-1]["x1"]) / 2

    deb, cre, sal = center("DEBITO"), center("CREDITO"), center("SALDO")
    combte = center("COMBTE")
    if None in (deb, cre, sal):
        return None
    return {
        "deb_cre": (deb + cre) / 2,     # límite entre columna Débito y Crédito
        "cre_sal": (cre + sal) / 2,     # límite entre Crédito y Saldo
        "amount_min": deb - 60,         # a la izquierda de esto es descripción
        "combte_max": (combte + 20) if combte else 106,
    }


def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    movs: List[dict] = []
    saldo_anterior: Optional[float] = None
    layout = None
    in_table = False
    finished = False
    desc_x = None   # x donde arranca la descripción (para las líneas de detalle)

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            if finished:
                break
            for lw in _group_lines(page.extract_words()):
                text = _clean(" ".join(w["text"] for w in lw))
                up = text.upper()

                lay = _header_layout(lw)
                if lay:
                    layout = lay
                    in_table = True
                    continue

                if not in_table or layout is None:
                    continue

                if up.startswith("SALDO ANTERIOR"):
                    amts = [w for w in lw if AMOUNT_RE.match(w["text"])]
                    if amts and saldo_anterior is None:
                        saldo_anterior = _to_float(amts[-1]["text"])
                    continue

                if up.startswith("SALDO AL"):
                    in_table = False
                    finished = True
                    break

                if "CONTINUA EN PAGINA" in up or "VIENE DE PAGINA" in up or up.startswith("PAGINA"):
                    continue

                first = lw[0]["text"]
                amount_words = [w for w in lw if AMOUNT_RE.match(w["text"]) and w["x0"] >= layout["amount_min"]]

                if DATE_RE.match(first):
                    rest = lw[1:]
                    combte = ""
                    if rest and COMBTE_RE.match(rest[0]["text"]) and rest[0]["x1"] <= layout["combte_max"]:
                        combte = rest[0]["text"]
                        rest = rest[1:]
                    desc_words = [w for w in rest if w not in amount_words]
                    if desc_words:
                        desc_x = desc_words[0]["x0"]

                    deb = cre = 0.0
                    saldo_inf = None
                    for w in amount_words:
                        xc = w["x1"]   # importes alineados a la derecha
                        if xc < layout["deb_cre"] + 25:
                            deb += _to_float(w["text"])
                        elif xc < layout["cre_sal"] + 25:
                            cre += _to_float(w["text"])
                        else:
                            saldo_inf = _to_float(w["text"])

                    movs.append({
                        "fecha": first,
                        "desc": _clean(" ".join(w["text"] for w in desc_words)),
                        "combte": combte,
                        "deb": deb,
                        "cre": cre,
                        "saldo_inf": saldo_inf,
                    })
                    continue

                # Línea de detalle (segundo renglón de la descripción)
                if movs and not amount_words and desc_x is not None and abs(lw[0]["x0"] - desc_x) < 4:
                    movs[-1]["desc"] = f"{movs[-1]['desc']} / {text}"

    cols = ["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]
    if not movs:
        return pd.DataFrame(columns=cols)

    saldo = saldo_anterior or 0.0
    out = []
    for m in movs:
        saldo = round(saldo - m["deb"] + m["cre"], 2)
        desc = m["desc"] or "(sin descripción)"
        # Control contra el saldo que informa el banco al cierre del día
        if m["saldo_inf"] is not None and abs(m["saldo_inf"] - saldo) > 0.01:
            desc = f"{desc} [REVISAR: el banco informa saldo {m['saldo_inf']:,.2f}]".replace(",", "X").replace(".", ",").replace("X", ".")
            saldo = m["saldo_inf"]
        out.append([m["fecha"], desc, round(m["deb"], 2), round(m["cre"], 2), saldo])

    df = pd.DataFrame(out, columns=cols)
    df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%y", errors="coerce")
    return df
