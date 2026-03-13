# process_bbva_v3.py — Parser BBVA (AR) con pdfplumber
# Ajustes:
# 1) Para renglones tipo "IMP.LEY ...." sin importes explícitos, si hay saldo nuevo, usar
# la diferencia de saldo respecto del saldo anterior como importe (crédito/débito).
# 2) Corte de cuenta robusto: frena al detectar frases que indican otras secciones
# ("ENVIADAS ACEPTADAS", "LE INFORMAMOS QUE", "INVERSIONES EN BONOS/FONDOS/ACCIONES",
# "LEGALES Y AVISOS", etc.).

from __future__ import annotations

import io
import math
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pdfplumber
DATE_RE = re.compile(r"\b([0-3]?\d)[/-]([01]?\d)(?:[/-](\d{2,4}))?\b")
AMT_IN_TEXT_RE = re.compile(r"[-+]?\s*\$?\s*(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d{2})(?:-)?")
AMOUNT_RE_STRICT = re.compile(r"^\s*\$?\s*[-+]?(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d{2})?\s*-?\s*$")

CURRENCY_STRIP = re.compile(r"[\s\$]")

HEADER_YEAR_RE = re.compile(r"\b(20\d{2}|19\d{2})\b", re.IGNORECASE)

ACC_HEADER_RE = re.compile(
    r"\bCC\s+(U\$S|\$)\s+([0-9][0-9\.\-\s]*)[/-]\s*([0-9]+)\b",
    re.IGNORECASE
)

MOV_HEADER_RE = re.compile(r"\bMOVIMIENTOS\s+EN\s+CUENTAS\b", re.IGNORECASE)
END_ACC_RE = re.compile(r"\b(SALDO\s+AL|TOTAL\s+MOVIMIENTOS)\b", re.IGNORECASE)
SALDO_ANT_RE = re.compile(r"\bSALDO\s+ANTERIOR\b", re.IGNORECASE)
HARD_END_MARKERS = tuple(s.upper() for s in [
    "ENVIADAS ACEPTADAS",
    "LE INFORMAMOS QUE",
    "INVERSIONES EN BONOS",
    "INVERSIONES EN FONDOS",
    "INVERSIONES EN ACCIONES",
    "LEGALES Y AVISOS",
    "DETALLE DE IMPUESTO",
    "TOTAL SALDOS DISPONIBLES",
])
NOISE_RE = re.compile(r"\(cid:\d+\)|cid:\d+", re.IGNORECASE)
LEADING_DUP_DATE_RE = re.compile(r"^\s*\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\s+")

BLOCKLIST_DESC = tuple(s.upper() for s in [
    "WWW.BBVA",
    "LOS DEPÓSITOS EN PESOS",
    "PÁGINA",
    "CUENTAS Y PAQUETES",
    "CUENTAS Y PAQUETE",
    "OCASA",
    "R.N.P.S.P."
])
IGNORE_LINE_MARKERS = (
    "SIN MOVIMIENTOS",
    "RECIBIDAS (INFORMACIÓN AL",
    "RECIBIDAS (INFORMACION AL",
    "CONSULTAS Y RECLAMOS",
)
try:
    from sklearn.cluster import KMeans
    HAS_SKLEARN = True
except Exception:
    HAS_SKLEARN = False
@dataclass
class PageColumns:
    cut1: float
    cut2: float
    left_border: float
def _clean_amount(txt: str) -> Optional[float]:
    if not txt:
        return None

    s = CURRENCY_STRIP.sub("", txt).replace(".", "").replace(",", ".").strip()

    neg = False
    if s.endswith("-"):
        neg = True
        s = s[:-1].strip()

    if s.startswith("-"):
        neg = True
        s = s[1:].strip()

    if s.startswith("+"):
        s = s[1:].strip()

    try:
        val = float(s)
    except ValueError:
        return None

    return -val if neg else val
