import io
import re
import pandas as pd
import pdfplumber

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")


def _to_float_ar(s: str) -> float:
    s = str(s).replace("$", "").replace("U$D", "").replace("U$S", "").replace(" ", "").replace("−", "-")
    neg = s.endswith("-")
    if neg:
        s = s[:-1]
    s = s.replace(".", "").replace(",", ".")
    v = float(s)
    return -v if neg else v


def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    rows = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(use_text_flow=False, keep_blank_chars=False)

            # agrupar por línea
            line_map = {}
            for w in words:
                y = round(w["top"], 1)
                key = None
                for k in list(line_map.keys()):
                    if abs(k - y) <= 1.5:
                        key = k
                        break
                if key is None:
                    key = y
                    line_map[key] = []
                line_map[key].append(w)

            for y in sorted(line_map.keys()):
                ws = sorted(line_map[y], key=lambda z: z["x0"])
                if not ws:
                    continue

                first_text = ws[0]["text"].strip()
                if not DATE_RE.match(first_text):
                    continue

                fecha = first_text

                # Columnas reales del PDF:
                # Fecha | Nro Ref | Causal | Concepto | Importe | Saldo
                # Se ignoran Nro Ref y Causal
                concepto_words = [w["text"] for w in ws if 185 <= w["x0"] < 410]
                importe_words = [w["text"] for w in ws if 410 <= w["x0"] < 520]
                saldo_words = [w["text"] for w in ws if w["x0"] >= 520]

                if not concepto_words or not importe_words or not saldo_words:
                    continue

                concepto = " ".join(concepto_words).strip()
                importe_txt = "".join(importe_words).strip()
                saldo_txt = "".join(saldo_words).strip()

                # limpiar prefijo monetario de cuentas U$D/U$S
                importe_txt = importe_txt.replace("U$D", "").replace("U$S", "").strip()
                saldo_txt = saldo_txt.replace("U$D", "").replace("U$S", "").strip()

                try:
                    importe = _to_float_ar(importe_txt)
                    saldo = _to_float_ar(saldo_txt)
                except Exception:
                    continue

                debito = abs(importe) if importe < 0 else 0.0
                credito = importe if importe > 0 else 0.0

                rows.append([fecha, concepto, debito, credito, saldo])

    # Orden invertido completo del PDF:
    # último movimiento del resumen -> primero en Excel
    rows = list(reversed(rows))

    df = pd.DataFrame(rows, columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo"])
    if not df.empty:
        df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%Y", errors="coerce")
    return df