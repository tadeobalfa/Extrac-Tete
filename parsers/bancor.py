import io, re, os
import pandas as pd
import pdfplumber
from typing import Optional

# ====== Config de límites (ajustables por entorno) ======
MAX_PAGES = int(os.getenv("EXTRACTOS_MAX_PAGES", "0"))       # 0 = sin límite
MAX_LINES = int(os.getenv("EXTRACTOS_MAX_LINES", "60000"))   # seguridad

# ====== Regex ======
MONEY_RE = re.compile(r"([\-−]?\d{1,3}(?:\.\d{3})*,\d{2})")
DATE_START_RE = re.compile(r"^(\d{1,2}/\d{2})\b")
SKIP_MOVEMENT = ["SALDO RES. ANTERIOR","SALDO RES ANTERIOR","SALDO RESUMEN ANTERIOR","SALDO FINAL","SALDO INICIAL"]

STOP_PHRASES = [
    "RESUMEN DE CUENTA","Banco de la Provincia de Córdoba","Banco de la Provincia de Cordoba",
    "Los depósitos en pesos y en moneda extranjera","En ningún caso, el total de garantía",
    "Com. \"A\"","La Provincia de Córdoba se constituye en garante","Se presume la conformidad del Cliente",
    "Usted  puede solicitar la \"Caja de ahorros\"","Usted puede consultar el \"Régimen de Transparencia\"",
    "www.bancor.com.ar","San Jerónimo 166","San Jeronimo 166","C.U.I.T","Periodo",
    "Total Impuesto al Valor Agregado:","IMPORTANTE: A partir de Enero/2018",
]

def _is_header_line(s: str) -> bool:
    u = (s or "").strip(); U = u.upper()
    if not u: return True
    if any(ph.upper() in U for ph in STOP_PHRASES): return True
    if "HTTP://" in U or "HTTPS://" in U or "WWW." in U: return True
    if len(u) > 180 and (u.count(",") + u.count(".") > 6): return True
    return False

def _to_float(s: str) -> float:
    s = (s or "").strip().replace("−","-")
    neg=False
    if s.endswith("-"): neg=True; s=s[:-1]
    s = s.replace(".","").replace(",",".").replace("$","").replace(" ","")
    try: v=float(s)
    except: v=0.0
    return -v if neg else v

def _extract_text(bts: bytes) -> str:
    out=[]; page_count=0
    with pdfplumber.open(io.BytesIO(bts)) as pdf:
        for p in pdf.pages:
            page_count += 1
            if MAX_PAGES and page_count > MAX_PAGES: break
            out.append(p.extract_text() or "")
    return "\n".join(out)

def _year(text: str) -> Optional[int]:
    m = re.search(r"\b\d{1,2}/\d{2}/(\d{4})\b", text)
    return int(m.group(1)) if m else None

def _initial_balance(text: str) -> Optional[float]:
    pats = [
        r"SALDO\s+RES\.\s+ANTERIOR.*?([\-−]?\d{1,3}(?:\.\d{3})*,\d{2})",
        r"SALDO\s+RES\s+ANTERIOR.*?([\-−]?\d{1,3}(?:\.\d{3})*,\d{2})",
        r"SALDO\s+RESUMEN\s+ANTERIOR.*?([\-−]?\d{1,3}(?:\.\d{3})*,\d{2})",
        r"SALDO\s+INICIAL.*?([\-−]?\d{1,3}(?:\.\d{3})*,\d{2})",
    ]
    for pat in pats:
        m = re.search(pat, text, flags=re.IGNORECASE|re.DOTALL)
        if m:
            s = m.group(1).replace("−","-")
            return float(s.replace(".","").replace(",","."))
    return None

def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    raw = _extract_text(file_bytes)
    year = _year(raw) or 2025
    lines = [ln.rstrip() for ln in raw.splitlines()]
    prev = _initial_balance(raw)
    rows=[]; i=0; line_count=0

    while i < len(lines):
        line_count += 1
        if line_count > MAX_LINES: break

        line = (lines[i] or "").strip(); i+=1
        if not DATE_START_RE.match(line): continue
        monies = list(MONEY_RE.finditer(line))
        if not monies: continue

        saldo = _to_float(monies[-1].group(1))
        date_token = DATE_START_RE.match(line).group(1)
        desc = line[len(date_token):monies[-1].start()].strip()
        desc = re.sub(MONEY_RE, "", desc)
        desc = re.sub(r"\s{2,}", " ", desc).strip(" -/")

        if any(k in desc.upper() for k in SKIP_MOVEMENT):
            prev = saldo
            continue

        # continuaciones (máx 4, sin montos, sin headers)
        parts=[]; peek=i
        while peek < len(lines):
            nxt=(lines[peek] or "").strip()
            if DATE_START_RE.match(nxt) or MONEY_RE.search(nxt) or _is_header_line(nxt): break
            if nxt: parts.append(nxt)
            if len(parts)>=4: break
            peek+=1
        i=max(i,peek)
        if parts: desc=(desc + " / " + " / ".join(parts)).strip(" /")

        dd,mm = date_token.split("/")
        fecha = f"{int(dd):02d}/{int(mm):02d}/{year}"

        deb=cred=0.0
        if prev is not None:
            delta = saldo - prev
            if delta < 0: deb = round(-delta,2)
            elif delta > 0: cred = round(delta,2)
        prev = saldo

        rows.append([fecha, desc, deb, cred, saldo])

    return pd.DataFrame(rows, columns=["Fecha","Descripción","Débito","Crédito","Saldo"])
