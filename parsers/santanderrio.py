# parsers/santanderrio.py
import io
import re

import pandas as pd
import pdfplumber


DATE_START = re.compile(r"^\d{2}/\d{2}/\d{2}")
MONEY_RE = re.compile(r"[−-]?\$?\s?\d{1,3}(?:\.\d{3})*,\d{2}-?")
SALDO_INICIAL_RE = re.compile(r"saldo\s+inicial", re.IGNORECASE)
STOP_RE = re.compile(r"^\s*Saldo total(?! en cuentas)", re.IGNORECASE)
PAGE_FOOTER_RE = re.compile(r"^\s*\d+\s*-\s*\d+\s*$")

# Porcentajes tipo "4,50%" o "0,6 %": el regex de montos los confundía con importes.
PERCENT_RE = re.compile(r"\d{1,3}(?:,\d+)?\s*%")

# Líneas de detalle de retenciones/percepciones (SIRCREB, etc.):
#   "Resp:30717568024 / 4,50% sobre $200.730,96"
# El monto que traen es la BASE de cálculo, no un saldo ni un movimiento.
DETAIL_LINE_RE = re.compile(
    r"^(?:\d{2}/\d{2}/\d{2}\s+)?(?:Resp(?:onsable)?\s*:|.*%\s*sobre\s*\$)",
    re.IGNORECASE,
)


# Encabezado de cuenta, se repite en cada página:
#   "Cuenta Corriente Nº 205-009387/9 CBU: 0720205820000000938790"
#   "Cuenta Corriente especial U$S Nº 205-020527/6 CBU: ..."
ACCOUNT_HEADER_RE = re.compile(
    r"^(?P<tipo>.+?)\s+N\s*[º°o]\.?\s*(?P<num>[\d\-/]+)\s+CBU",
    re.IGNORECASE,
)
SECTION_PESOS_RE = re.compile(r"^\s*Movimientos en pesos", re.IGNORECASE)
SECTION_USD_RE = re.compile(r"^\s*Movimientos en d[oó]lares", re.IGNORECASE)
# A partir de acá ya no hay movimientos (resumen impositivo / leyendas)
END_RE = re.compile(r"^\s*Detalle impositivo", re.IGNORECASE)
NO_MOVS_RE = re.compile(r"No ten[eé]s movimientos", re.IGNORECASE)


def account_label(tipo: str, numero: str, usd: bool) -> str:
    """
    Nombre de la hoja para cada cuenta: tipo + moneda + últimos 3 dígitos.
    Ej: "Cta Cte PESOS 879", "Cta Cte Esp DOLARES 276" (máx. 31 caracteres).
    """
    t = re.sub(r"\s+", " ", tipo).strip()
    usd = usd or bool(re.search(r"U\$S|USD|D[OÓ]LAR", t, re.IGNORECASE))
    t = re.sub(r"\b(U\$S|USD|en d[oó]lares|en pesos|\$)", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"(?i)cuenta corriente", "Cta Cte", t)
    t = re.sub(r"(?i)caja de ahorros?", "Caja Ahorro", t)
    t = re.sub(r"(?i)\bespecial\b", "Esp", t)
    t = re.sub(r"\s+", " ", t).strip() or "Cuenta"
    digits = re.sub(r"\D", "", numero)
    label = f"{t} {'DOLARES' if usd else 'PESOS'} {digits[-3:]}".strip()
    return label[:31]


def money_in(line: str):
    """Montos de la línea, ignorando porcentajes."""
    return MONEY_RE.findall(PERCENT_RE.sub(" ", line))

EXCLUDE_TOKENS = (
    "Acuerdo:",
    "Vencimiento:",
    "Fecha Comprobante Movimiento",
    "Banco Santander Argentina",
    "Salvo error u omisión",
)


def is_header_summary(line: str) -> bool:
    if PAGE_FOOTER_RE.match(line):
        return True
    return any(tok in line for tok in EXCLUDE_TOKENS)


def clean_text(line: str) -> str:
    t = line.strip()
    # Proteger porcentajes ("0,6%") para que no los borre el regex de montos
    pcts = []
    def _keep(m):
        pcts.append(m.group(0))
        return f"\x00{len(pcts)-1}\x00"
    t = PERCENT_RE.sub(_keep, t)
    t = MONEY_RE.sub("", t)
    t = re.sub(r"\x00(\d+)\x00", lambda m: pcts[int(m.group(1))], t)

    if DATE_START.match(t):
        t = t[8:].strip()

    t = re.sub(r"^\d{3,}\s+", "", t)
    t = t.replace("$", " ")
    t = re.sub(r"\s{2,}", " ", t)

    return t.strip(" /-").strip()


def to_number(s: str) -> float:
    if not s:
        return 0.0

    t = str(s).strip()
    t = t.replace("−", "-").replace("–", "-").replace("—", "-")
    t = t.replace("$", "")
    t = re.sub(r"[\u00A0\u202F\s]", "", t)

    neg = False

    if t.startswith("-"):
        neg = True
        t = t[1:]

    if t.endswith("-"):
        neg = True
        t = t[:-1]

    t = t.replace(".", "").replace(",", ".")

    try:
        v = float(t)
    except Exception:
        v = 0.0

    return -v if neg else v


def _append_record(records, fecha, desc, saldo, prev_saldo, cuenta=""):
    if not fecha or not desc:
        return prev_saldo

    base = prev_saldo if prev_saldo is not None else saldo
    delta = round(saldo - base, 2)

    credito = max(delta, 0.0)
    debito = max(-delta, 0.0)

    records.append([
        fecha,
        desc.strip(),
        round(debito, 2),
        round(credito, 2),
        round(saldo, 2),
        cuenta,
    ])

    return saldo


def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    records = []

    current_date = None
    prev_saldo = None
    started = False
    stopped = False

    pending_date = None
    pending_desc_parts = []
    last_record_open = False

    # Multi-cuenta: un resumen puede traer, p.ej., una cuenta en pesos y
    # otra en dólares. Cada movimiento queda etiquetado con su cuenta y la
    # app arma una hoja del Excel por cuenta.
    section_usd = False
    current_account = ""
    acct_start = 0                 # índice en records donde arrancó la cuenta actual
    acct_info = {}                 # cuenta -> (fecha saldo inicial, saldo inicial)
    account_order = []

    def _open_account(label):
        nonlocal current_account, acct_start
        if label != current_account:
            current_account = label
            acct_start = len(records)
        if label not in account_order:
            account_order.append(label)

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        # No saltar la primera página:
        # en algunos Santander el Saldo Inicial y los movimientos arrancan en página 1.
        pages = pdf.pages

        for page in pages:
            text = page.extract_text() or ""

            for raw in text.split("\n"):
                line = raw.strip()

                if not line:
                    continue

                if END_RE.match(line):
                    stopped = True
                    break

                if SECTION_PESOS_RE.match(line):
                    section_usd = False
                    continue
                if SECTION_USD_RE.match(line):
                    section_usd = True
                    continue

                m_acc = ACCOUNT_HEADER_RE.match(line)
                if m_acc:
                    # Se repite al inicio de cada página: si es la misma cuenta, sigue.
                    _open_account(account_label(m_acc.group("tipo"), m_acc.group("num"), section_usd))
                    continue

                if is_header_summary(line) or NO_MOVS_RE.search(line):
                    continue

                # "Saldo total" cierra la cuenta actual (puede venir otra después).
                # El "Saldo total en cuentas al..." de la carátula se ignora.
                if STOP_RE.match(line):
                    started = False
                    prev_saldo = None
                    pending_date = None
                    pending_desc_parts = []
                    last_record_open = False
                    continue

                if SALDO_INICIAL_RE.search(line):
                    nums = money_in(line)
                    if nums:
                        prev_saldo = to_number(nums[-1])
                        started = True

                        if DATE_START.match(line):
                            current_date = line[:8]

                        if not current_account:
                            _open_account(account_label("Cuenta", "", section_usd))
                        acct_start = len(records)
                        acct_info.setdefault(current_account, (current_date, prev_saldo))

                    pending_date = None
                    pending_desc_parts = []
                    last_record_open = False
                    continue

                if not started:
                    continue

                # Detalle de retención (Resp: ... % sobre $base): siempre se
                # agrega a la descripción del movimiento anterior. Nunca es un
                # movimiento propio (antes generaba asientos inventados y
                # rompía la cadena de saldos).
                if DETAIL_LINE_RE.match(line):
                    # Se conserva la base ("sobre $200.730,96") como referencia
                    detail = re.sub(r"\s{2,}", " ", line[8:] if DATE_START.match(line) else line).strip()
                    if len(records) > acct_start and detail:
                        records[-1][1] = records[-1][1] + " / " + detail
                    pending_date = None
                    pending_desc_parts = []
                    last_record_open = True
                    continue

                nums = money_in(line)
                has_date = bool(DATE_START.match(line))

                if has_date:
                    current_date = line[:8]

                if has_date and not nums:
                    pending_date = current_date
                    desc = clean_text(line)
                    pending_desc_parts = [desc] if desc else []
                    last_record_open = False
                    continue

                if not nums:
                    detail = clean_text(line)

                    if detail:
                        # CASO SANTANDER:
                        # Si viene una línea sin fecha y sin importes después de un movimiento,
                        # es segunda línea descriptiva del movimiento anterior.
                        # Ej:
                        # Credito transf online banking emp
                        # De punto di sauce sa / factura - fac / 30711732272
                        if len(records) > acct_start and not has_date:
                            records[-1][1] = records[-1][1] + " / " + detail
                            last_record_open = True
                            continue
                            
                        # Si tiene fecha pero no importe, empieza un movimiento pendiente.
                        pending_desc_parts = [detail]
                        pending_date = current_date
                        last_record_open = False

                    continue

                # Líneas de detalle posteriores con montos de referencia,
                # por ejemplo: Responsable... / 1,30% sobre $10.400.000,00
                # No son movimientos nuevos.
                # IMPORTANTE: no incluir "%" como condición general,
                # porque movimientos reales como "Imp ley 25413 deb 0,075%"
                # también tienen porcentaje.
                if (
                    not has_date
                    and pending_date is None
                    and last_record_open
                    and len(records) > acct_start
                    and re.match(r"^(Responsable:|De |Por |Del )", line, re.IGNORECASE)
                ):
                    detail = clean_text(line)
                    if detail:
                        records[-1][1] = records[-1][1] + " / " + detail
                    continue

                saldo = to_number(nums[-1])
                desc_line = clean_text(line)

                if has_date:
                    mov_date = current_date
                    desc_parts = [desc_line] if desc_line else pending_desc_parts[:]
                else:
                    mov_date = pending_date or current_date
                    desc_parts = pending_desc_parts[:]

                    if desc_line:
                        desc_parts.append(desc_line)

                desc_final = " / ".join(
                    p.strip()
                    for p in desc_parts
                    if p and p.strip()
                )

                if not desc_final:
                    desc_final = "(sin descripción)"

                if not SALDO_INICIAL_RE.search(desc_final):
                    prev_saldo = _append_record(
                        records=records,
                        fecha=mov_date,
                        desc=desc_final,
                        saldo=saldo,
                        prev_saldo=prev_saldo,
                        cuenta=current_account,
                    )
                    last_record_open = True

                pending_date = None
                pending_desc_parts = []

            if stopped:
                break

    # Cuentas sin movimientos en el período: una fila informativa con el
    # saldo, para que igual aparezca su hoja en el Excel.
    with_movs = {r[5] for r in records}
    for cta in account_order:
        if cta not in with_movs and cta in acct_info:
            fecha_ini, saldo_ini = acct_info[cta]
            records.append([fecha_ini, "Sin movimientos en el período", 0.0, 0.0, round(saldo_ini, 2), cta])

    cols = ["Fecha", "Descripción", "Débito", "Crédito", "Saldo", "Cuenta"]
    df = pd.DataFrame(records, columns=cols)

    if not df.empty:
        df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%y", errors="coerce")
        # Mantener el orden de las cuentas como vienen en el PDF
        order = {c: i for i, c in enumerate(account_order)}
        df["_o"] = df["Cuenta"].map(order).fillna(len(order))
        df["_i"] = range(len(df))
        df = df.sort_values(["_o", "_i"]).drop(columns=["_o", "_i"]).reset_index(drop=True)

    return df[cols]
