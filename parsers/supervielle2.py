import io
import re
import pandas as pd
import pdfplumber

DATETIME_RE = re.compile(r"^\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}$")
MONEY_RE = re.compile(r"^[−-]?\$?\s?\d{1,3}(?:\.\d{3})*,\d{2}-?$")


def _clean_cell(x) -> str:
    if x is None:
        return ""
    s = str(x).replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _to_float(tok: str) -> float:
    if not tok:
        return 0.0
    t = tok.strip().replace("−", "-").replace("$", "").replace(" ", "")
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


def _is_header_row(cells) -> bool:
    txt = " | ".join(cells).upper()
    return (
        "FECHA" in txt
        and "CONCEPTO" in txt
        and "DETALLE" in txt
        and "SALDO" in txt
    )


def _join_desc(concepto: str, detalle: str) -> str:
    concepto = (concepto or "").strip()
    detalle = (detalle or "").strip()
    if concepto and detalle:
        return f"{concepto} - {detalle}"
    return concepto or detalle


def _normalize_row(raw_row):
    cells = [_clean_cell(c) for c in (raw_row or [])]
    if not any(cells):
        return None

    while len(cells) < 6:
        cells.append("")
    cells = cells[:6]

    return {
        "fecha_raw": cells[0],
        "concepto": cells[1],
        "detalle": cells[2],
        "deb_raw": cells[3],
        "cred_raw": cells[4],
        "saldo_raw": cells[5],
    }


def _has_datetime(row: dict) -> bool:
    return bool(DATETIME_RE.match((row.get("fecha_raw") or "").strip()))


def _has_amounts(row: dict) -> bool:
    return bool(
        (row.get("deb_raw") or "").strip()
        or (row.get("cred_raw") or "").strip()
        or (row.get("saldo_raw") or "").strip()
    )


def _append_text(base: str, extra: str) -> str:
    base = (base or "").strip()
    extra = (extra or "").strip()
    if not extra:
        return base
    if not base:
        return extra
    return f"{base} {extra}".strip()


def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    rows = []

    # Movimiento actualmente abierto
    current = None

    # Prefijos que quedaron cortados al final de una hoja
    pending_next_concept = ""
    pending_next_detail = ""

    table_settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "intersection_tolerance": 5,
        "snap_tolerance": 3,
        "join_tolerance": 3,
        "edge_min_length": 20,
        "min_words_vertical": 1,
        "min_words_horizontal": 1,
    }

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables(table_settings=table_settings) or []

            for table in tables:
                for raw_row in table:
                    row = _normalize_row(raw_row)
                    if row is None:
                        continue

                    cells = [
                        row["fecha_raw"],
                        row["concepto"],
                        row["detalle"],
                        row["deb_raw"],
                        row["cred_raw"],
                        row["saldo_raw"],
                    ]

                    if _is_header_row(cells):
                        continue

                    has_dt = _has_datetime(row)
                    has_amt = _has_amounts(row)

                    # -----------------------------
                    # 1) Nueva fila real con fecha
                    # -----------------------------
                    if has_dt:
                        if current is not None:
                            rows.append(current)

                        concepto = row["concepto"]
                        detalle = row["detalle"]

                        if pending_next_concept:
                            concepto = _append_text(pending_next_concept, concepto)
                            pending_next_concept = ""

                        if pending_next_detail:
                            detalle = _append_text(pending_next_detail, detalle)
                            pending_next_detail = ""

                        current = {
                            "fecha_raw": row["fecha_raw"],
                            "concepto": concepto,
                            "detalle": detalle,
                            "deb_raw": row["deb_raw"],
                            "cred_raw": row["cred_raw"],
                            "saldo_raw": row["saldo_raw"],
                        }
                        continue

                    # --------------------------------------------------
                    # 2) Fila sin fecha ni importes, con solo concepto:
                    #    normalmente es prefijo del PRÓXIMO movimiento
                    #    (ej. "COMIS." o "Impuesto Débitos y")
                    # --------------------------------------------------
                    if row["concepto"] and not row["detalle"] and not has_amt:
                        pending_next_concept = _append_text(pending_next_concept, row["concepto"])
                        continue

                    # --------------------------------------------------
                    # 3) Fila sin fecha, sin concepto, con solo detalle:
                    #    puede ser continuación del detalle del ANTERIOR
                    #    o prefijo del SIGUIENTE si el actual era un
                    #    impuesto sin detalle
                    # --------------------------------------------------
                    if (not row["concepto"]) and row["detalle"] and not has_amt:
                        if (
                            current is not None
                            and not current.get("detalle")
                            and "IMPUESTO DÉBITOS Y CRÉDITOS" in (current.get("concepto", "").upper())
                        ):
                            pending_next_detail = _append_text(pending_next_detail, row["detalle"])
                        elif current is not None:
                            current["detalle"] = _append_text(current.get("detalle", ""), row["detalle"])
                        else:
                            pending_next_detail = _append_text(pending_next_detail, row["detalle"])
                        continue

                    # --------------------------------------------------
                    # 4) Fila sin fecha, con concepto y detalle, sin importes:
                    #    suele ser continuación textual del movimiento actual
                    # --------------------------------------------------
                    if row["concepto"] and row["detalle"] and not has_amt:
                        txt = _append_text(row["concepto"], row["detalle"])
                        if current is not None:
                            current["detalle"] = _append_text(current.get("detalle", ""), txt)
                        else:
                            pending_next_detail = _append_text(pending_next_detail, txt)
                        continue

                    # --------------------------------------------------
                    # 5) Fila sin fecha pero con importes:
                    #    si falta algo en el actual, completar
                    # --------------------------------------------------
                    if has_amt and not has_dt:
                        if current is not None:
                            if row["deb_raw"] and not current["deb_raw"]:
                                current["deb_raw"] = row["deb_raw"]
                            if row["cred_raw"] and not current["cred_raw"]:
                                current["cred_raw"] = row["cred_raw"]
                            if row["saldo_raw"] and not current["saldo_raw"]:
                                current["saldo_raw"] = row["saldo_raw"]
                        continue

        if current is not None:
            rows.append(current)

    # El PDF viene de más reciente a más antiguo.
    # Se invierte TODO el documento completo.
    rows = list(reversed(rows))

    out = []
    for r in rows:
        fecha = pd.to_datetime(r["fecha_raw"], format="%Y/%m/%d %H:%M", errors="coerce")
        descripcion = _join_desc(r["concepto"], r["detalle"])
        out.append([
            fecha,
            descripcion,
            _to_float(r["deb_raw"]),
            _to_float(r["cred_raw"]),
            _to_float(r["saldo_raw"]),
        ])

    return pd.DataFrame(out, columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo"])