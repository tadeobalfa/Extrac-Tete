# parsers/supervielle.py
import io
import re
from datetime import datetime
from statistics import mean
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import pdfplumber

# --------- Patrones ----------
DATE_RE  = re.compile(r"^(?P<d>\d{1,2})/(?P<m>\d{1,2})(?:/(?P<y>\d{2,4}))?$")
MONEY_RE = re.compile(r"^-?\d{1,3}(?:\.\d{3})*,\d{2}-?$")  # acepta '1.234,56-'
ACCT_RE  = re.compile(r"numero de cuenta\s+([0-9\-]+/\d+)", re.IGNORECASE)

# Footer / boilerplate típico (lista acotada para no comer líneas válidas)
FOOTER_PHRASES = [
    "resumen de cuenta", "página", "pagina", "clave bancaria unica",
    "importante:", "circular", "bcra", "ley 24.485", "com. \"a\"",
    "consumidor final", "crédito fiscal", "credito fiscal",
    "se presumirá conformidad", "se presumira conformidad",
]

# Frases/keywords donde debe cortarse la descripción (boilerplate/legales)
TRUNCATE_AFTER = [
    "imp ley 25413", "reg de recaudacion sircreb", "reg de recaudación sircreb", "sircreb",
    "saldo periodo actual", "saldo período actual",
    "los depósitos en pesos", "los depositos en pesos",
    "operaciones a nombre de dos o más personas", "operaciones a nombre de dos o mas personas",
    "la garantía", "la garantia",
    "se encuentran excluidos", "hayan contado con incentivos",
    "retribuciones especiales", "por endoso",
    "personas vinculadas", "cajero automático", "cajero automatico",
    "para mayor información", "para mayor informacion",
    "monotributistas", "régimen de sostenimiento e inclusión fiscal",
    "regimen de sostenimiento e inclusion fiscal", "27.618", "27618"
]

# --------- Utils ----------
def clean_text(t: str) -> str:
    t = t.replace("\xa0"," ").replace("\u2009"," ").replace("\u202f"," ")
    return re.sub(r"\s+"," ", t).strip()

def is_footer(text: str) -> bool:
    t = text.lower().strip()
    if any(p in t for p in FOOTER_PHRASES):
        return True
    return len(t) > 260 and " " in t  # párrafos legales muy largos

def truncate_legal_desc(text: str) -> str:
    if not text:
        return text
    t_low = text.lower()
    cut_pos = None
    for key in TRUNCATE_AFTER:
        idx = t_low.find(key)
        if idx != -1 and (cut_pos is None or idx < cut_pos):
            cut_pos = idx
    return text[:cut_pos].strip() if cut_pos is not None else text

def parse_amount(s: str) -> Optional[float]:
    """'1.234,56' y '1.234,56-' -> float (negativo si termina en '-')"""
    s = s.strip().replace("−","-")
    neg_trailing = s.endswith("-")
    if neg_trailing:
        s = s[:-1]
    s = s.replace(".", "").replace(",", ".")
    try:
        v = float(s)
        return -v if neg_trailing else v
    except Exception:
        return None

def build_sheet_token(acct: str) -> str:
    # "19-00619265/2" -> "CTA 9265-2"
    try:
        base, suf = acct.split("/")
        digits = re.sub(r"\D","", base)
        last4 = digits[-4:] if len(digits)>=4 else digits
        return f"CTA {last4}-{suf}"
    except Exception:
        return f"CTA {acct[-7:]}".replace("/", "-")

def kmeans_1d(xs, k=3, iters=12):
    xs = sorted(xs)
    if len(xs) < k:
        while len(xs) < k:
            xs.append(xs[-1] if xs else 0.0)
    qs = [xs[int(len(xs)*p)] for p in (0.2, 0.5, 0.8)]
    centers = qs[:k]
    for _ in range(iters):
        buckets = {i: [] for i in range(k)}
        for x in xs:
            i = min(range(k), key=lambda j: abs(x - centers[j]))
            buckets[i].append(x)
        for i in range(k):
            if buckets[i]:
                centers[i] = mean(buckets[i])
    centers = sorted(centers)
    b1 = (centers[0] + centers[1]) / 2
    b2 = (centers[1] + centers[2]) / 2
    return centers, (b1, b2)  # límites entre bandas

def calc_cuts_from_money(words):
    """Cortes por PÁGINA usando SOLO importes (x1):
       desc_cut = min(x1) - margen ; deb_cut=b1 ; cre_cut=b2."""
    xs1 = [w["x1"] for w in words if MONEY_RE.match(w["text"])]
    if len(xs1) < 2:
        return None
    _, (b1, b2) = kmeans_1d(xs1, k=3, iters=12)
    desc_cut = min(xs1) - 4.0   # margen pequeño
    return (desc_cut, b1, b2)

def near(a, b, tol=18.0):
    return abs(a - b) <= tol

# --------- Parser principal (unificado) ----------
def parse_pdf(file_bytes: bytes, default_year: Optional[int] = None) -> pd.DataFrame:
    """
    Devuelve un DF con columnas: Fecha | Descripción | Débito | Crédito | Saldo
    - Une descripciones en varios renglones
    - Corta textos legales/boilerplate
    - Usa cortes por posición (k-means) robustos a PDFs grandes
    - Prefija la cuenta en la Descripción: "[CTA 9265-2] ..."
    """
    if default_year is None:
        default_year = datetime.today().year

    rows: List[List] = []
    current_acct: Optional[str] = None
    acct_token: str = ""

    global_cuts = None  # (desc_cut, deb_cut, cre_cut) de la 1ª página con movimientos
    cuts_prev   = None  # cortes vigentes para heredar

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(
                x_tolerance=2, y_tolerance=3,
                keep_blank_chars=False, use_text_flow=True
            )

            # --- CORTES: globales + por página con seguridad ---
            page_cuts = calc_cuts_from_money(words)

            if global_cuts is None and page_cuts is not None:
                global_cuts = page_cuts

            if page_cuts and global_cuts:
                d1, db1, cr1 = page_cuts
                d0, db0, cr0 = global_cuts
                cuts = page_cuts if (near(db1, db0) and near(cr1, cr0)) else global_cuts
            else:
                cuts = page_cuts or global_cuts or cuts_prev

            if cuts is None:
                continue

            cuts_prev = cuts
            desc_cut_x1, deb_cut_x1, cre_cut_x1 = cuts

            # corte auxiliar por x0 para NO cortar palabras de descripción
            money_x0 = [w["x0"] for w in words if MONEY_RE.match(w["text"])]
            desc_cut_x0 = (min(money_x0) - 2.0) if money_x0 else desc_cut_x1

            # agrupar por línea
            lines: Dict[float, List[dict]] = {}
            for w in words:
                y = round(w["top"], 1)
                lines.setdefault(y, []).append(w)

            for y in sorted(lines.keys()):
                toks = sorted(lines[y], key=lambda w: w["x0"])
                line_txt = clean_text(" ".join([w["text"] for w in toks]))
                if not line_txt:
                    continue

                # Detectar cambio de cuenta
                m_ac = ACCT_RE.search(line_txt)
                if m_ac:
                    current_acct = m_ac.group(1)
                    acct_token = build_sheet_token(current_acct)
                    continue
                if current_acct is None:
                    continue

                # descartar footers/legales larguísimos
                if is_footer(line_txt):
                    continue

                # Si NO arranca con fecha → podría ser continuación de descripción
                first = toks[0]["text"]
                if not DATE_RE.match(first):
                    if rows:
                        cont = [
                            tw["text"] for tw in toks
                            if ((tw["x1"] <= desc_cut_x1 or tw["x0"] <= desc_cut_x0)
                                and not MONEY_RE.match(tw["text"]))
                        ]
                        if cont:
                            # anexar a la última fila (misma cuenta)
                            last = rows[-1]
                            last_desc = last[1]
                            merged = clean_text(f"{last_desc} {' '.join(cont)}")
                            last[1] = truncate_legal_desc(merged)
                    continue

                # === Nueva fila ===
                # Fecha
                m = DATE_RE.match(first); fecha_dt = None
                if m:
                    d = int(m.group("d")); mo = int(m.group("m"))
                    yy = m.group("y")
                    if yy:
                        yy = int(yy)
                        yy = (yy + 2000) if yy < 100 else yy
                    else:
                        yy = default_year
                    try:
                        fecha_dt = datetime(yy, mo, d)
                    except Exception:
                        fecha_dt = None

                # Descripción (sin cortar palabras) + truncado de legales + prefijo cuenta
                desc_tokens = [
                    tw["text"] for tw in toks[1:]
                    if ((tw["x1"] <= desc_cut_x1 or tw["x0"] <= desc_cut_x0)
                        and not MONEY_RE.match(tw["text"]))
                ]
                descripcion = truncate_legal_desc(clean_text(" ".join(desc_tokens)))
                if acct_token:
                    descripcion = f"[{acct_token}] {descripcion}".strip()

                # Débito / Crédito / Saldo por posición (x1)
                deb = 0.0; cre = 0.0; saldo = np.nan
                for tw in toks:
                    txt = tw["text"]
                    if not MONEY_RE.match(txt):
                        continue
                    val = parse_amount(txt)
                    x1 = tw["x1"]
                    if x1 <= deb_cut_x1:
                        if val is not None: deb = val
                    elif x1 <= cre_cut_x1:
                        if val is not None: cre = val
                    else:
                        if val is not None: saldo = val

                rows.append([fecha_dt, descripcion, deb, cre, saldo])

    df = pd.DataFrame(rows, columns=["Fecha","Descripción","Débito","Crédito","Saldo"])
    return df
