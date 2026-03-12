# parsers/santanderrio.py
import io, re, pdfplumber, pandas as pd

DATE_START = re.compile(r"^\d{2}/\d{2}/\d{2}")
MONEY_RE   = re.compile(r"[−-]?\$?\s?\d{1,3}(?:\.\d{3})*,\d{2}-?")
SALDO_INICIAL_RE = re.compile(r"saldo\s+inicial", re.IGNORECASE)
STOP_RE    = re.compile(r"^\s*Saldo total(?! en cuentas)", re.IGNORECASE)
EXCLUDE_TOKENS = ("Cuenta Corriente N°", "CBU:", "Acuerdo:", "Vencimiento:")

def is_header_summary(line: str) -> bool:
    return any(tok in line for tok in EXCLUDE_TOKENS)

def clean_text(line: str) -> str:
    t = line.strip()
    t = MONEY_RE.sub("", t)
    if DATE_START.match(t):
        t = t[8:].strip()
    t = re.sub(r"^\d{3,}\s+", "", t)  # quita códigos al inicio
    t = re.sub(r"\s{2,}", " ", t)
    return t.strip()

def to_number(s: str) -> float:
    if not s: return 0.0
    t = s.strip().replace("−","-").replace("–","-").replace("—","-")
    t = t.replace("$","")
    t = re.sub(r"[\u00A0\u202F\s]", "", t)
    neg = False
    if t.startswith("-"): neg, t = True, t[1:]
    if t.endswith("-"):  neg, t = True, t[:-1]
    t = t.replace(".","").replace(",",".")
    try: v = float(t)
    except: v = 0.0
    return -v if neg else v

def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    records = []
    current_date = None
    prev_saldo = None
    started = False
    stopped = False
    open_desc, open_saldo = None, None

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        # En la individual, los movimientos inician en página 2; conservamos esto
        pages = pdf.pages[1:] if len(pdf.pages) > 1 else pdf.pages
        for page in pages:
            text = page.extract_text() or ""
            for raw in text.split("\n"):
                line = raw.strip()
                if not line: continue
                if is_header_summary(line): continue

                if STOP_RE.match(line):
                    if open_saldo is not None and open_desc:
                        delta = open_saldo - (prev_saldo if prev_saldo is not None else open_saldo)
                        credito = max(delta, 0.0); debito = max(-delta, 0.0)
                        records.append([current_date, open_desc, debito, credito, open_saldo])
                        prev_saldo = open_saldo
                    stopped = True
                    break

                if not started and SALDO_INICIAL_RE.search(line):
                    nums = MONEY_RE.findall(line)
                    if nums:
                        prev_saldo = to_number(nums[-1])
                        started = True
                    continue
                if not started: 
                    continue

                if DATE_START.match(line):
                    current_date = line[:8]  # dd/mm/yy

                nums = MONEY_RE.findall(line)
                if nums:
                    # cerrar movimiento previo si estaba abierto
                    if open_saldo is not None and open_desc:
                        delta = open_saldo - (prev_saldo if prev_saldo is not None else open_saldo)
                        credito = max(delta, 0.0); debito = max(-delta, 0.0)
                        records.append([current_date, open_desc, debito, credito, open_saldo])
                        prev_saldo = open_saldo
                    header = clean_text(line)
                    open_desc = header if header else "(sin descripción)"
                    open_saldo = to_number(nums[-1])
                else:
                    # sublínea del mismo movimiento
                    detail = clean_text(line)
                    if detail and open_desc is not None:
                        open_desc = open_desc + " / " + detail
            if stopped: break

    if not stopped and open_saldo is not None and open_desc:
        delta = open_saldo - (prev_saldo if prev_saldo is not None else open_saldo)
        credito = max(delta, 0.0); debito = max(-delta, 0.0)
        records.append([current_date, open_desc, debito, credito, open_saldo])

    df = pd.DataFrame(records, columns=["Fecha","Descripción completa","Débito","Crédito","Saldo"])
    if not df.empty:
        df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%y", errors="coerce")

    # Adaptar a la unificada
    df = df.rename(columns={"Descripción completa": "Descripción"})
    return df[["Fecha","Descripción","Débito","Crédito","Saldo"]]
