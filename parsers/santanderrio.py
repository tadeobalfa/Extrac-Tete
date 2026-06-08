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

EXCLUDE_TOKENS = (
    "Cuenta Corriente N°",
    "Cuenta Corriente Nº",
    "CBU:",
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
    t = MONEY_RE.sub("", t)

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


def _append_record(records, fecha, desc, saldo, prev_saldo):
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

                if is_header_summary(line):
                    continue

                # IMPORTANTE:
                # "Saldo total al..." aparece arriba del resumen antes de Movimientos.
                # Solo debe cortar cuando ya empezó la lectura real.
                if STOP_RE.match(line):
                    if started:
                        stopped = True
                        break
                    continue

                if SALDO_INICIAL_RE.search(line):
                    nums = MONEY_RE.findall(line)
                    if nums:
                        prev_saldo = to_number(nums[-1])
                        started = True

                        if DATE_START.match(line):
                            current_date = line[:8]

                    pending_date = None
                    pending_desc_parts = []
                    last_record_open = False
                    continue

                if not started:
                    continue

                nums = MONEY_RE.findall(line)
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
                        if records and not has_date:
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
                if (
                    not has_date
                    and pending_date is None
                    and last_record_open
                    and records
                    and (
                        "%" in line
                        or re.match(r"^(Responsable:|De |Por |Del )", line, re.IGNORECASE)
                    )
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
                    )
                    last_record_open = True

                pending_date = None
                pending_desc_parts = []

            if stopped:
                break

    df = pd.DataFrame(
        records,
        columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo"],
    )

    if not df.empty:
        df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%y", errors="coerce")

    return df[["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]]
