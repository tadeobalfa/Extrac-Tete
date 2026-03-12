import io
import re
import pandas as pd
import pdfplumber
from typing import List, Tuple, Optional

DATE_RE = re.compile(r"^\d{1,2}/\d{2}/\d{2,4}$")
MONEY_RE = re.compile(r"^[−-]?\$?\s?\d{1,3}(?:\.\d{3})*,\d{2}-?$", re.UNICODE)
SALDO_ACT_RE = re.compile(r"SALDO\s+ACTUAL", re.IGNORECASE)

HDR_DEB_TOKENS = {"DEBITO", "DEBITOS", "DÉBITO", "DÉBITOS"}
HDR_CRED_TOKENS = {"CREDITO", "CREDITOS", "CRÉDITO", "CRÉDITOS"}
HDR_SALDO_TOKENS = {"SALDO", "SALDOS"}

STOP_TITLES = [
    "TRANSFERENCIAS RECIBIDAS",
    "DETALLE - COMISION DE PAQUETES / MANTENIMIENTO DE CUENTAS",
    "SITUACION IMPOSITIVA",
]


def to_float_ar(tok: str) -> float:
    t = tok.replace("−", "-").replace("$", "").replace(" ", "")
    neg = False
    if t.endswith("-"):
        neg, t = True, t[:-1]
    if t.startswith("-"):
        neg, t = True, t[1:]
    t = t.replace(".", "").replace(",", ".")
    try:
        v = float(t)
    except Exception:
        v = 0.0
    return -v if neg else v


def normalize_date(date_str: str) -> str:
    d, m, y = date_str.split("/")
    y = int(y)
    if y < 100:
        y = 2000 + y if y < 70 else 1900 + y
    return f"{int(d):02d}/{int(m):02d}/{y:04d}"


def split_three_bands(xcoords: List[float]) -> Optional[Tuple[float, float]]:
    xs = sorted(xcoords)
    dedup = []
    for x in xs:
        if not dedup or abs(x - dedup[-1]) > 10:
            dedup.append(x)
    xs = dedup
    if len(xs) < 3:
        return None

    gaps = [(xs[i + 1] - xs[i], i) for i in range(len(xs) - 1)]
    top2 = sorted(gaps, reverse=True)[:2]
    idxs = sorted([g[1] for g in top2])

    b1 = (xs[idxs[0]] + xs[idxs[0] + 1]) / 2.0
    b2 = (xs[idxs[1]] + xs[idxs[1] + 1]) / 2.0
    if b1 > b2:
        b1, b2 = b2, b1
    return b1, b2


def aligned_headers(words, first_row_y, tol_y=3.0):
    cand = []
    for w in words:
        t = w["text"].strip().upper()
        if t in HDR_DEB_TOKENS | HDR_CRED_TOKENS | HDR_SALDO_TOKENS:
            if w["top"] <= first_row_y + 12:
                cand.append((t, (w["x0"] + w["x1"]) / 2.0, w["top"]))

    groups = {}
    for t, cx, y in cand:
        key = None
        for ky in list(groups.keys()):
            if abs(ky - y) <= tol_y:
                key = ky
                break
        if key is None:
            key = y
            groups[key] = []
        groups[key].append((t, cx))

    best = None
    for _yk, arr in groups.items():
        tags = {t for t, _ in arr}
        if (tags & HDR_DEB_TOKENS) and (tags & HDR_CRED_TOKENS):
            best = arr
            break

    if not best:
        return None, None, None

    x_deb = x_cred = x_saldo = None
    for t, cx in best:
        if t in HDR_DEB_TOKENS:
            x_deb = cx
        elif t in HDR_CRED_TOKENS:
            x_cred = cx
        elif t in HDR_SALDO_TOKENS:
            x_saldo = cx

    return x_deb, x_cred, x_saldo


def _normalize_spaces(s: str) -> str:
    return re.sub(r"\s{2,}", " ", s or "").strip()


def _sheet_name_from_account_header(row_text: str) -> Optional[str]:
    txt = _normalize_spaces(row_text).upper()

    if "CBU" not in txt:
        return None

    if ("CUENTA CORRIENTE" not in txt) and ("CCTE ESP" not in txt):
        return None

    digits = re.sub(r"\D", "", txt)
    suf = digits[-3:] if len(digits) >= 3 else "000"

    if "CCTE ESP" in txt and "DOLAR" in txt:
        return f"CCTE ESP USD {suf}"
    if "CCTE ESP" in txt:
        return f"CCTE ESP {suf}"
    if "CUENTA CORRIENTE" in txt and "DOLAR" in txt:
        return f"CCTE USD {suf}"
    if "CUENTA CORRIENTE" in txt and "PESOS" in txt:
        return f"CCTE PESOS {suf}"

    return f"CTA {suf}"


def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    rows = []
    account_initial_balance = {}
    account_order = []

    current_account = None
    account_active = False

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(use_text_flow=False, keep_blank_chars=False)
            if not words:
                continue

            # agrupar por Y
            yrows = {}
            for w in words:
                ykey = round(w["top"], 2)
                yrows.setdefault(ykey, []).append(w)

            # primera fila con fecha
            date_ys = []
            for y, ws in yrows.items():
                ws_sorted = sorted(ws, key=lambda ww: ww["x0"])
                if ws_sorted and DATE_RE.match(ws_sorted[0]["text"].strip()):
                    date_ys.append(y)

            if not date_ys:
                continue

            first_row_y = min(date_ys)

            # cortes por encabezados o clustering
            x_deb, x_cred, x_saldo = aligned_headers(words, first_row_y, tol_y=3.0)
            if x_deb and x_cred:
                cut1 = (x_deb + x_cred) / 2.0
                cut2 = (x_cred + (x_saldo if x_saldo else page.width * 0.96)) / 2.0
                if cut2 <= cut1:
                    cut2 = page.width * 0.92
            else:
                x_candidates = []
                for y, ws in yrows.items():
                    ws_sorted = sorted(ws, key=lambda ww: ww["x0"])
                    if ws_sorted and DATE_RE.match(ws_sorted[0]["text"].strip()):
                        for ww in ws_sorted[1:]:
                            if MONEY_RE.match(ww["text"].strip()):
                                x_candidates.append((ww["x0"] + ww["x1"]) / 2.0)

                bands = split_three_bands(x_candidates)
                if bands:
                    cut1, cut2 = bands
                else:
                    cut1, cut2 = page.width * 0.55, page.width * 0.80

            DEB_RIGHT = cut1 - 3.0
            CRED_LEFT = DEB_RIGHT
            CRED_RIGHT = cut2 + 4.0
            SALDO_LEFT = cut2 + 6.0

            for y in sorted(yrows.keys()):
                ws = sorted(yrows[y], key=lambda ww: ww["x0"])
                if not ws:
                    continue

                row_text = _normalize_spaces(" ".join(w["text"].strip() for w in ws))
                row_up = row_text.upper()

                # Detectar inicio de nueva cuenta
                detected_account = _sheet_name_from_account_header(row_text)
                if detected_account:
                    current_account = detected_account
                    account_active = True
                    if current_account not in account_order:
                        account_order.append(current_account)
                    continue

                # Si aparece un título que ya no pertenece a la cuenta, cortar bloque
                if any(title in row_up for title in STOP_TITLES):
                    account_active = False
                    continue

                if not account_active or current_account is None:
                    continue

                # Si aparece SALDO ACTUAL, termina la cuenta y no se toma como movimiento
                if SALDO_ACT_RE.search(row_up):
                    account_active = False
                    continue

                first = ws[0]["text"].strip()
                if not DATE_RE.match(first):
                    continue

                # Capturar SALDO ANTERIOR / SALDO INICIAL por cuenta, pero no como movimiento
                if "SALDO ANTERIOR" in row_up or "SALDO INICIAL" in row_up:
                    money_right = [ww for ww in ws if MONEY_RE.match(ww["text"].strip())]
                    if money_right:
                        account_initial_balance[current_account] = to_float_ar(
                            max(money_right, key=lambda ww: ww["x1"])["text"].strip()
                        )
                    continue

                fecha = pd.to_datetime(
                    normalize_date(first),
                    format="%d/%m/%Y",
                    errors="coerce"
                )

                desc_parts = []
                deb = 0.0
                cred = 0.0

                monies = []
                idx_local = 0
                for ww in ws[1:]:
                    t = ww["text"].strip()
                    cx = (ww["x0"] + ww["x1"]) / 2.0
                    if MONEY_RE.match(t):
                        monies.append((idx_local, t, cx))
                    else:
                        if cx < cut1:
                            desc_parts.append(t)
                    idx_local += 1

                used = set()

                # primera pasada (sin solape)
                for i, t, cx in monies:
                    if cx < DEB_RIGHT:
                        deb += abs(to_float_ar(t))
                        used.add(i)
                    elif CRED_LEFT <= cx < CRED_RIGHT:
                        cred += abs(to_float_ar(t))
                        used.add(i)
                    elif cx >= SALDO_LEFT:
                        used.add(i)  # saldo -> ignorar/reconocer, no reusar

                # rescate por fila
                remaining = [(i, t, cx) for (i, t, cx) in monies if i not in used]
                if remaining:
                    left = [(i, t, cx) for (i, t, cx) in remaining if cx < DEB_RIGHT]
                    mid = [(i, t, cx) for (i, t, cx) in remaining if CRED_LEFT <= cx < CRED_RIGHT]

                    if cred == 0.0 and mid:
                        cred = sum(abs(to_float_ar(t)) for _, t, _ in mid)
                        used.update([i for i, _, _ in mid])

                    if deb == 0.0 and left:
                        deb = sum(abs(to_float_ar(t)) for _, t, _ in left)
                        used.update([i for i, _, _ in left])

                    if (deb == 0.0 or cred == 0.0) and len(remaining) == 2:
                        cand = [
                            (i, t, cx)
                            for (i, t, cx) in remaining
                            if i not in used and cx < SALDO_LEFT
                        ]
                        if len(cand) == 2:
                            (iR, tR, _xR), (iL, tL, _xL) = sorted(
                                cand, key=lambda x: x[2], reverse=True
                            )
                            if cred == 0.0:
                                cred = abs(to_float_ar(tR))
                                used.add(iR)
                            if deb == 0.0:
                                deb = abs(to_float_ar(tL))
                                used.add(iL)

                if deb == cred and deb != 0.0:
                    usable = [(t, cx) for (i, t, cx) in monies if i in used and (cx < SALDO_LEFT)]
                    if len(usable) == 1:
                        t1, cx1 = usable[0]
                        val = abs(to_float_ar(t1))
                        if cx1 >= cut1:
                            deb, cred = 0.0, val
                        else:
                            deb, cred = val, 0.0

                desc = _normalize_spaces(" ".join(desc_parts))
                rows.append((current_account, fecha, desc, deb, cred))

    df = pd.DataFrame(rows, columns=["Cuenta", "Fecha", "Descripción completa", "Débito", "Crédito"])

    if df.empty:
        return pd.DataFrame(columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo", "Cuenta"])

    out_parts = []
    ordered_accounts = [cta for cta in account_order if cta in df["Cuenta"].unique()]

    for cuenta in ordered_accounts:
        chunk = df[df["Cuenta"] == cuenta].copy().reset_index(drop=True)
        saldo_ini = account_initial_balance.get(cuenta, 0.0)

        row_ini = pd.DataFrame([{
            "Cuenta": cuenta,
            "Fecha": pd.NaT,
            "Descripción completa": "SALDO INICIAL",
            "Débito": 0.0,
            "Crédito": 0.0,
            "Saldo": saldo_ini
        }])

        chunk = pd.concat([row_ini, chunk], ignore_index=True)
        out_parts.append(chunk)

    df_out = pd.concat(out_parts, ignore_index=True)
    df_out = df_out.rename(columns={"Descripción completa": "Descripción"})
    return df_out[["Fecha", "Descripción", "Débito", "Crédito", "Saldo", "Cuenta"]]