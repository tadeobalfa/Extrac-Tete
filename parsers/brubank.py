# parsers/brubank.py
# Parser Brubank
# Salida: Fecha | Descripción | Débito | Crédito | Saldo | Cuenta

import io
import re
from typing import List, Optional, Tuple

import pandas as pd
import pdfplumber


# -------------------------
# Helpers
# -------------------------
def _clean_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_money(s: str) -> float:
    if s is None:
        return 0.0
    s = str(s).strip()
    if not s or s == "-":
        return 0.0

    s = s.replace("$", "").replace(" ", "").replace("\xa0", "")
    neg = False

    # negativo por guion final o inicial
    if s.endswith("-"):
        neg = True
        s = s[:-1]
    if s.startswith("-"):
        neg = True
        s = s[1:]

    s = s.replace(".", "").replace(",", ".")
    try:
        v = float(s)
    except Exception:
        return 0.0
    return -v if neg else v


def _parse_date(s: str):
    s = _clean_text(s)
    if not s:
        return pd.NaT
    try:
        return pd.to_datetime(s, format="%d-%m-%y", errors="raise")
    except Exception:
        return pd.to_datetime(s, dayfirst=True, errors="coerce")


def _currency_code(moneda: str) -> str:
    m = _clean_text(moneda).upper()
    if "USD" in m or "DOLAR" in m or "DÓLAR" in m:
        return "USD"
    return "ARS"


def _build_sheet_name(tipo: str, moneda: str, numero: str) -> str:
    tipo_u = _clean_text(tipo).upper()
    code = _currency_code(moneda)

    # si es remunerada, priorizar nombre corto consistente
    if "REMUNERADA" in tipo_u:
        return f"{code} Remunerada"

    # si hay número, usar últimos 5 dígitos
    digs = re.sub(r"\D", "", str(numero or ""))
    if digs:
        return f"{code} {digs[-5:]}"

    return code


def _extract_account_info(lines: List[str]) -> Tuple[str, str, str]:
    """
    Devuelve:
    - tipo
    - moneda
    - numero
    """
    tipo = ""
    moneda = ""
    numero = ""

    for ln in lines:
        ln_c = _clean_text(ln)

        m = re.search(r"Tipo\s+(.*?)\s+Saldo Inicial\b", ln_c, flags=re.I)
        if m:
            tipo = _clean_text(m.group(1))

        m = re.search(r"Moneda\s+(.*?)\s+Créditos\b", ln_c, flags=re.I)
        if m:
            moneda = _clean_text(m.group(1))

        m = re.search(r"Número\s+([0-9]+)", ln_c, flags=re.I)
        if m:
            numero = _clean_text(m.group(1))

    return tipo, moneda, numero


def _is_noise_line(line: str) -> bool:
    s = _clean_text(line).upper()
    if not s:
        return True

    noise_prefixes = (
        "MOVIMIENTOS",
        "FECHA #REF DESCRIPCIÓN DÉBITO CRÉDITO SALDO",
        "FECHA #REF DESCRIPCION DEBITO CREDITO SALDO",
        "MI CUENTA RESUMEN",
        "TIPO ",
        "MONEDA ",
        "CUIT ",
        "NÚMERO ",
        "NUMERO ",
        "CBU ",
        "IMP. TRANS. FINANCIERAS ",
        "SALDO INICIAL ",
        "CRÉDITOS ",
        "DEBITOS ",
        "DÉBITOS ",
        "SALDO FINAL ",
    )
    if s.startswith(noise_prefixes):
        return True

    if re.match(r"^\d{1,2}\s+[A-Z]{3}\s+\d{4}\s+AL\s+\d{1,2}\s+[A-Z]{3}\s+\d{4}$", s):
        return True

    # Filtrar sólo líneas de domicilio muy específicas del encabezado,
    # no cualquier descripción que contenga "CORDOBA"
    address_like = (
        s.startswith("BELGRANO ")
        or s.startswith("X5000")
        or s.startswith("X50")
    )
    if address_like:
        return True

    return False


def _parse_movement_line(line: str):
    """
    Espera algo tipo:
    02-01-25 1059609344 ImpuestoL Imp.Ley 25413 s/de $ 1.200,00 - $ 171.657,48
    02-01-25 1059559452 De una cuenta tuya - - $ 500.000,00 $ 389.963,76
    03-01-25 1060608201 Fondeo de tu cuenta remunerada $ 319.261,00 - - $ 115.675,67

    Devuelve:
    (fecha, descripcion, debito, credito, saldo) o None
    """
    line = _clean_text(line)
    if not line:
        return None

    m = re.match(r"^(?P<fecha>\d{2}-\d{2}-\d{2})\s+(?P<ref>\d+)\s+(?P<resto>.+)$", line)
    if not m:
        return None

    fecha = _parse_date(m.group("fecha"))
    resto = _clean_text(m.group("resto"))

    # desde el final: débito | crédito | signo saldo opcional | saldo
    # ejemplo:
    #   desc $ 1.200,00 - $ 171.657,48
    #   desc - $ 500.000,00 $ 389.963,76
    #   desc $ 303.305,00 - - $ 89,47
    patt = re.compile(
        r"^(?P<desc>.*?)\s+"
        r"(?P<deb>\$?\s*\d[\d.]*,\d{2}|-)\s+"
        r"(?P<cred>\$?\s*\d[\d.]*,\d{2}|-)\s+"
        r"(?:(?P<saldo_neg>-)\s+)?"
        r"(?P<saldo>\$?\s*\d[\d.]*,\d{2})$"
    )

    m2 = patt.match(resto)
    if not m2:
        return None

    desc = _clean_text(m2.group("desc"))
    deb = _parse_money(m2.group("deb"))
    cred = _parse_money(m2.group("cred"))
    saldo = _parse_money(m2.group("saldo"))

    if m2.group("saldo_neg"):
        saldo = -abs(saldo)

    return fecha, desc, deb, cred, saldo


# -------------------------
# Main
# -------------------------
def parse_pdf(raw_bytes: bytes) -> pd.DataFrame:
    rows = []

    current_account = "GENERAL"
    current_tipo = ""
    current_moneda = ""
    current_numero = ""

    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            if not text.strip():
                continue

            lines = [_clean_text(x) for x in text.splitlines() if _clean_text(x)]

            # Si la página trae cabecera de cuenta, actualizar cuenta actual
            joined = "\n".join(lines).upper()
            if "MI CUENTA" in joined and "RESUMEN" in joined:
                tipo, moneda, numero = _extract_account_info(lines)
                if tipo or moneda or numero:
                    current_tipo = tipo or current_tipo
                    current_moneda = moneda or current_moneda
                    current_numero = numero or current_numero
                    current_account = _build_sheet_name(current_tipo, current_moneda, current_numero)

            in_movs = False
            last_idx = None

            for ln in lines:
                ln_u = ln.upper()

                if "FECHA #REF DESCRIPCIÓN DÉBITO CRÉDITO SALDO" in ln_u or "FECHA #REF DESCRIPCION DEBITO CREDITO SALDO" in ln_u:
                    in_movs = True
                    continue

                if not in_movs:
                    continue

                if _is_noise_line(ln):
                    continue

                mov = _parse_movement_line(ln)
                if mov is not None:
                    fecha, desc, deb, cred, saldo = mov
                    rows.append(
                        {
                            "Fecha": fecha,
                            "Descripción": desc,
                            "Débito": deb,
                            "Crédito": cred,
                            "Saldo": saldo,
                            "Cuenta": current_account,
                        }
                    )
                    last_idx = len(rows) - 1
                else:
                    # continuación de descripción
                    if last_idx is not None:
                        extra = _clean_text(ln)
                        if extra and not _is_noise_line(extra):
                            rows[last_idx]["Descripción"] = _clean_text(
                                rows[last_idx]["Descripción"] + " " + extra
                            )

    df = pd.DataFrame(rows, columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo", "Cuenta"])

    if df.empty:
        return pd.DataFrame(columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo", "Cuenta"])

    # limpieza final
    df["Descripción"] = df["Descripción"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    df["Cuenta"] = df["Cuenta"].astype(str).str.strip().replace("", "GENERAL")

    return df