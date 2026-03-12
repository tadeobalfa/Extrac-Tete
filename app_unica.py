# app_unica.py — Extrac-Tete · Convertidor de Extractos Bancarios
# Ejecutar: streamlit run app_unica.py

import io
import re
import time
import hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from typing import Callable, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import pdfplumber
import streamlit as st

# ==== Parsers por banco ====
from parsers.bancor import parse_pdf as parse_bancor
from parsers.galicia import parse_pdf as parse_galicia
from parsers.icbc import parse_pdf as parse_icbc
from parsers.macro import parse_pdf as parse_macro
from parsers.macro2 import parse_pdf as parse_macro2
from parsers.nacion import parse_pdf as parse_nacion
from parsers.patagonia import parse_pdf as parse_patagonia
from parsers.santanderrio import parse_pdf as parse_santanderrio
from parsers.supervielle import parse_pdf as parse_supervielle
from parsers.supervielle2 import parse_pdf as parse_supervielle2
from parsers.mercadopago import parse_pdf as parse_mp
from parsers.credicoop import parse_pdf as parse_credicoop
from parsers.bbva import parse_pdf as parse_bbva  # BBVA usa motor original 1:1
from parsers.brubank import parse_pdf as parse_brubank

# ====== UI ======
st.set_page_config(page_title="Extrac-Tete · Convertidor de Extractos", page_icon="💳", layout="wide")
st.markdown("""
<style>

/* ===== Ocultar barra superior de Streamlit ===== */
header {visibility: hidden;}
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

[data-testid="stHeader"] {
  background: transparent !important;
}

[data-testid="stToolbar"] {
  display: none !important;
}

/* ===== Variables ===== */
:root {
  --bg1:#0f071c;
  --bg2:#1a0f2b;
  --ink:#e9dff9;
  --muted:#b8a9d9;
  --card:rgba(255,255,255,.05);
  --stroke:rgba(255,255,255,.08);
  --accent1:#ff3ea5;
  --accent2:#ffc53d;
  --accent3:#9b5cff;
}

/* ===== Fondo general ===== */
html, body, .stApp {
  background:
    radial-gradient(1200px 600px at 20% -10%, #1b1030 0%, var(--bg1) 35%),
    radial-gradient(1400px 800px at 90% -20%, #281343 0%, var(--bg1) 40%),
    var(--bg1) !important;
  color: var(--ink);
  font-weight: 500;
}

.block-container {
  padding-top: 24px;
  max-width: 1100px;
}

/* ===== Hero ===== */
.h-hero {
  display:flex;
  align-items:center;
  gap:14px;
  margin: 10px 0 12px 0;
  overflow: visible;
}

.logo-pill {
  width:58px;
  height:58px;
  border-radius:16px;
  background: linear-gradient(180deg,#2b164a,#1b0f2c);
  border:1px solid var(--stroke);
  display:flex;
  align-items:center;
  justify-content:center;
  box-shadow: 0 8px 24px rgba(0,0,0,.35), inset 0 0 0 1px rgba(255,255,255,.04);
  font-size:28px;
}

.h-title {
  font-size:46px;
  line-height:1.05;
  margin:0;
  padding-top:6px;
  letter-spacing:.5px;
  background: linear-gradient(90deg,#ff3ea5,#ff8a56 35%,#ffc53d 70%);
  -webkit-background-clip:text;
  background-clip:text;
  -webkit-text-fill-color:transparent;
  font-weight:800;
  display:inline-block;
  overflow:visible;
}

.h-sub {
  font-size:20px;
  opacity:.9;
  margin: 2px 0 10px 0;
  color:#cbb6f3;
}

.h-note {
  font-size:14px;
  opacity:.75;
  margin-bottom:14px;
}

/* ===== Card ===== */
.card {
  border:1px solid var(--stroke);
  background: var(--card);
  border-radius: 16px;
  padding: 18px 16px 8px 16px;
  box-shadow: 0 10px 30px rgba(0,0,0,.35), inset 0 0 0 1px rgba(255,255,255,.02);
}

/* ===== Inputs ===== */
.stSelectbox > div > div,
.stFileUploader > div {
  background: rgba(255,255,255,.06);
  border: 1px solid rgba(255,255,255,.1);
  border-radius: 12px;
}

/* ===== Uploader oscuro ===== */
[data-testid="stFileUploaderDropzone"] {
  background: rgba(255,255,255,.05) !important;
  border: 2px dashed rgba(255,255,255,.15) !important;
  border-radius: 14px !important;
  color: white !important;
}

[data-testid="stFileUploaderDropzone"] * {
  color: #e9dff9 !important;
}

.stFileUploader section {
  background: transparent !important;
}

/* ===== Checkboxes ===== */
.stCheckbox > label,
.stCheckbox > div > label {
  color: var(--ink) !important;
}

input[type="checkbox"] {
  accent-color: var(--accent3);
}

/* ===== Tabs ===== */
.stTabs [data-baseweb="tab"] {
  color: var(--muted);
  font-weight: 600;
}

.stTabs [data-baseweb="tab"][aria-selected="true"] {
  color: var(--ink);
  border-bottom: 2px solid var(--accent1);
}

/* ===== Botones ===== */
.stButton > button {
  height: 48px;
  font-weight: 800;
  border-radius: 12px;
  background: linear-gradient(90deg,var(--accent1), #ff7a5f 45%, var(--accent2) 95%);
  color: #1c102d;
  border: none;
  box-shadow: 0 10px 28px rgba(255,62,165,.35);
}

.stButton > button:hover {
  filter: brightness(1.03);
  transform: translateY(-0.5px);
}

/* ===== Dataframes ===== */
.stDataFrame {
  border-radius: 12px;
  overflow: hidden;
}

/* ===== Autor ===== */
.author {
  display:flex;
  justify-content:flex-end;
  margin-top: 10px;
}

.author .tiny {
  font-size: 12px;
  opacity:.9;
  text-align:center;
}

.author .name {
  font-weight: 800;
}

</style>
""", unsafe_allow_html=True)
st.markdown("""
<div class="h-hero">
    <div class="logo-pill">💳</div>
    <div>
        <div class="h-title">Extrac-Tete</div>
        <div class="h-sub">Convertidor de Extractos Bancarios</div>
    </div>
</div>
<div class="h-note">
Elegí el banco y subí al menos un PDF. Configurá las opciones y convertí a Excel.
</div>
""", unsafe_allow_html=True)

# ===== Utilidades =====
EXPECTED_COLS = ["Fecha", "Descripción", "Clasificacion", "Débito", "Crédito", "Saldo"]

CLASSIF_FILE_CANDIDATES = [
    Path(__file__).resolve().parent / "CLASIFICACION EXTRACTOS.xlsx",
    Path("/mnt/data/CLASIFICACION EXTRACTOS.xlsx"),
]

BANK_RULE_SHEETS = {
    "BANCOR": "BANCOR",
    "GALICIA": "GALICIA",
    "ICBC": "ICBC",
    "PATAGONIA": "PATAGONIA",
    "MACRO": "MACRO",
    "MACRO 2": "MACRO 2",
    "NACION": "NACION",
    "SANTANDER RIO": "SANTANDER RIO",
    "SUPERVIELLE": "SUPERVIELLE",
    "SUPERVIELLE 2": "SUPERVIELLE 2",
    "MERCADO PAGO": "MERCADO PAGO",
    "CREDICOOP": "CREDICOOP",
    "BBVA": "BBVA",
    "BRUBANK": "BRUBANK",
}

def _norm_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).upper().strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _find_classif_file() -> Optional[Path]:
    for p in CLASSIF_FILE_CANDIDATES:
        if p.exists():
            return p
    return None

@st.cache_data(show_spinner=False)
def _load_classification_rules(_mtime: Optional[float] = None):
    path = _find_classif_file()
    if path is None:
        return {"BANK": {}, "GENERAL": []}

    xls = pd.ExcelFile(path)

    bank_rules: Dict[str, List[dict]] = {}
    general_rules: List[dict] = []

    for bank_name, sheet_name in BANK_RULE_SHEETS.items():
        if sheet_name not in xls.sheet_names:
            bank_rules[bank_name] = []
            continue

        df_sheet = pd.read_excel(path, sheet_name=sheet_name)
        df_sheet.columns = [str(c).strip() for c in df_sheet.columns]

        desc_col = None
        class_col = None
        for c in df_sheet.columns:
            cu = _norm_text(c)
            if cu in {"DESCRIPCIÓN", "DESCRIPCION"}:
                desc_col = c
            elif cu == "CLASIFICACION":
                class_col = c

        rules = []
        if desc_col and class_col:
            for idx, row in df_sheet.iterrows():
                pattern = _norm_text(row.get(desc_col, ""))
                clasif = str(row.get(class_col, "")).strip()
                if not pattern or pattern == "NAN" or not clasif or clasif.upper() == "NAN":
                    continue
                rules.append({
                    "pattern": pattern,
                    "clasificacion": clasif,
                    "priority_len": len(pattern),
                    "order": idx,
                })

        bank_rules[bank_name] = rules

    if "GENERAL" in xls.sheet_names:
        df_gen = pd.read_excel(path, sheet_name="GENERAL")
        df_gen.columns = [str(c).strip() for c in df_gen.columns]

        desc_col = None
        dc_col = None
        class_col = None

        for c in df_gen.columns:
            cu = _norm_text(c)
            if cu in {"DESCRIPCIÓN", "DESCRIPCION"}:
                desc_col = c
            elif cu == "DEBITO/CREDITO":
                dc_col = c
            elif cu == "CLASIFICACION":
                class_col = c

        if desc_col and class_col:
            for idx, row in df_gen.iterrows():
                pattern = _norm_text(row.get(desc_col, ""))
                dc = _norm_text(row.get(dc_col, "")) if dc_col else ""
                clasif = str(row.get(class_col, "")).strip()

                if not pattern or pattern == "NAN" or not clasif or clasif.upper() == "NAN":
                    continue

                general_rules.append({
                    "pattern": pattern,
                    "debcred": dc,
                    "clasificacion": clasif,
                    "priority_len": len(pattern),
                    "order": idx,
                })

    return {"BANK": bank_rules, "GENERAL": general_rules}

def _classify_row(bank: str, descripcion: str, debito: float, credito: float, rules_pack: dict) -> str:
    desc = _norm_text(descripcion)
    if not desc:
        return ""

    bank_candidates = []
    for rule in rules_pack["BANK"].get(bank, []):
        if rule["pattern"] in desc:
            bank_candidates.append(rule)

    if bank_candidates:
        bank_candidates = sorted(
            bank_candidates,
            key=lambda r: (-r["priority_len"], r["order"])
        )
        return bank_candidates[0]["clasificacion"]

    general_candidates = []
    for rule in rules_pack["GENERAL"]:
        if rule["pattern"] not in desc:
            continue

        cond = rule.get("debcred", "")
        if cond == "DEBITO" and float(debito) <= 0:
            continue
        if cond == "CREDITO" and float(credito) <= 0:
            continue

        general_candidates.append(rule)

    if general_candidates:
        general_candidates = sorted(
            general_candidates,
            key=lambda r: (-r["priority_len"], r["order"])
        )
        return general_candidates[0]["clasificacion"]

    return "SIN CLASIFICAR"

def _apply_classification(df: pd.DataFrame, bank: str) -> pd.DataFrame:
    df = df.copy()

    if "Clasificacion" not in df.columns:
        df["Clasificacion"] = ""

    path = _find_classif_file()
    mtime = path.stat().st_mtime if path is not None else None
    rules_pack = _load_classification_rules(mtime)

    if df.empty:
        return df

    mask_visual = (
        df["Descripción"].astype(str).str.startswith("===")
        | (
            df["Fecha"].isna()
            & df["Descripción"].astype(str).str.strip().eq("")
            & (df[["Débito", "Crédito", "Saldo"]].abs().sum(axis=1) == 0)
        )
    )

    visual_idx = set(df.index[mask_visual].tolist())

    clasifs = []
    for idx, row in df.iterrows():
        if idx in visual_idx:
            clasifs.append("")
            continue

        clasifs.append(
            _classify_row(
                bank=bank,
                descripcion=row.get("Descripción", ""),
                debito=float(row.get("Débito", 0.0) or 0.0),
                credito=float(row.get("Crédito", 0.0) or 0.0),
                rules_pack=rules_pack,
            )
        )

    df["Clasificacion"] = clasifs
    return df

def _ensure_columns_for_export(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    rename = {}
    for col in df.columns:
        low = str(col).strip().lower()
        if low in {"descripcion completa", "descripción completa"}:
            rename[col] = "Descripción"
        elif low == "clasificacion":
            rename[col] = "Clasificacion"
    if rename:
        df = df.rename(columns=rename)

    if "Clasificacion" not in df.columns:
        df["Clasificacion"] = ""

    for c in EXPECTED_COLS:
        if c not in df.columns:
            if c in {"Débito", "Crédito", "Saldo"}:
                df[c] = 0.0
            else:
                df[c] = ""

    if "Cuenta" in df.columns:
        return df[EXPECTED_COLS + ["Cuenta"]]

    return df[EXPECTED_COLS]

def _bytes_hash(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()

@st.cache_data(show_spinner=False)
def _cached_parse(bank: str, file_hash: str, raw_bytes: bytes) -> pd.DataFrame:
    return PARSERS[bank](raw_bytes)

def _run_with_timeout(fn, *args, timeout: int = 120):
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fn, *args)
        return fut.result(timeout=timeout)

def _coerce_number(x) -> float:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0.0
    if isinstance(x, (int, float, np.number)):
        return float(x)
    s = str(x).strip().replace("\u2212", "-").replace("−", "-").replace("$", "").replace(" ", "")
    if s in ("", "-"):
        return 0.0
    neg = s.endswith("-")
    s = s[:-1] if neg else s
    if re.search(r",\d{2}$", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        v = float(s)
    except Exception:
        v = 0.0
    return -v if neg else v

def _coerce_date_any(x) -> pd.Timestamp:
    if isinstance(x, pd.Timestamp):
        return x
    s = "" if x is None else str(x).strip()
    if not s:
        return pd.NaT
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return pd.to_datetime(s, format=fmt, errors="raise")
        except Exception:
            pass
    return pd.to_datetime(s, dayfirst=True, errors="coerce")

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=EXPECTED_COLS)

    df = df.copy()
    rename = {}
    for col in df.columns:
        low = str(col).strip().lower()
        if low in {"fecha", "date"}:
            rename[col] = "Fecha"
        elif low in {"descripcion", "descripción", "concepto", "detalle", "descripcion completa", "descripción completa"}:
            rename[col] = "Descripción"
        elif low in {"clasificacion"}:
            rename[col] = "Clasificacion"
        elif low in {"debito", "débito", "debitos"}:
            rename[col] = "Débito"
        elif low in {"credito", "crédito", "creditos"}:
            rename[col] = "Crédito"
        elif low in {"saldo", "saldo actual", "balance"}:
            rename[col] = "Saldo"
        elif low in {"cuenta", "account", "cta"}:
            rename[col] = "Cuenta"

    if rename:
        df = df.rename(columns=rename)

    for c in EXPECTED_COLS:
        if c not in df.columns:
            if c in {"Débito", "Crédito", "Saldo"}:
                df[c] = np.nan
            else:
                df[c] = ""

    cols = EXPECTED_COLS + (["Cuenta"] if "Cuenta" in df.columns else [])
    df = df[cols]

    df["Fecha"] = df["Fecha"].apply(_coerce_date_any)
    for c in ["Débito", "Crédito", "Saldo"]:
        df[c] = df[c].apply(_coerce_number)

    df["Descripción"] = df["Descripción"].astype(str).str.strip().replace({"nan": ""})
    df["Clasificacion"] = df["Clasificacion"].astype(str).str.strip().replace({"nan": ""})
    if "Cuenta" in df.columns:
        df["Cuenta"] = df["Cuenta"].astype(str).str.strip()

    mask = (
        df["Fecha"].notna()
        | df["Descripción"].astype(str).str.len().gt(0)
        | (df[["Débito", "Crédito", "Saldo"]].abs().sum(axis=1) > 0)
    )
    return df[mask].reset_index(drop=True)

MES_MAP = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12
}

def _infer_period_from_filename(name: str):
    s = name.lower().replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")
    m = re.search(r"(20\d{2})\D+(\d{1,2})", s)
    if m:
        y, mm = int(m.group(1)), int(m.group(2))
        if 1 <= mm <= 12:
            return y, mm
    m = re.search(r"(\d{1,2})\D+(20\d{2})", s)
    if m:
        mm, y = int(m.group(1)), int(m.group(2))
        if 1 <= mm <= 12:
            return y, mm
    for mes_txt, mm in MES_MAP.items():
        if mes_txt in s:
            m2 = re.search(r"(20\d{2})", s)
            if m2:
                return int(m2.group(1)), mm
    return None, None

def _df_min_date(df: pd.DataFrame):
    if "Fecha" in df.columns:
        return pd.to_datetime(df["Fecha"], errors="coerce").min()
    return pd.NaT

# ===== Detección automática de banco =====
def _extract_text_for_detection(raw_bytes: bytes, max_pages: int = 3) -> str:
    parts = []
    try:
        with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
            for page in pdf.pages[:max_pages]:
                txt = page.extract_text() or ""
                if txt:
                    parts.append(txt)
    except Exception:
        return ""
    return "\n".join(parts).upper()

def detect_bank(raw_bytes: bytes) -> Optional[str]:
    txt = _extract_text_for_detection(raw_bytes, max_pages=3)

    if not txt.strip():
        return None

    if "BRUBANK" in txt and "MI CUENTA" in txt and "MOVIMIENTOS" in txt:
        return "BRUBANK"

    if "MERCADO PAGO" in txt or "MERCADOPAGO" in txt:
        return "MERCADO PAGO"

    if "BBVA" in txt and ("MOVIMIENTOS" in txt or "CUENTA" in txt):
        return "BBVA"

    if "BANCO GALICIA" in txt or ("GALICIA" in txt and "TOTAL" in txt):
        return "GALICIA"

    if "ICBC" in txt:
        return "ICBC"

    if "CREDICOOP" in txt:
        return "CREDICOOP"

    if "BANCO PATAGONIA" in txt or ("PATAGONIA" in txt and "SALDO ANTERIOR" in txt):
        return "PATAGONIA"

    if "BANCO DE LA NACION ARGENTINA" in txt or "BANCO NACION" in txt:
        return "NACION"

    if "SANTANDER" in txt or "SANTANDER RIO" in txt:
        return "SANTANDER RIO"

    if "SUPERVIELLE" in txt:
        return "SUPERVIELLE"

    if "BANCOR" in txt or "BANCO DE CORDOBA" in txt or "BANCO DE CÓRDOBA" in txt:
        return "BANCOR"

    if "BANCO MACRO" in txt or re.search(r"\bMACRO\b", txt):
        if "NRO. DE REFERENCIA" in txt and "CAUSAL" in txt and "IMPORTE" in txt:
            return "MACRO 2"
        return "MACRO"

    return None

# ==== Fixes por banco ====
def fix_bancor(df):
    up = df["Descripción"].str.upper()
    return df[~(up.str.contains("SALDO RES") | up.str.contains("SALDO FINAL") | up.str.contains("SALDO INICIAL"))].reset_index(drop=True)

def fix_patagonia(df):
    df = df.copy()

    if "Cuenta" not in df.columns:
        df["Cuenta"] = "GENERAL"

    out = []

    for cuenta, chunk in df.groupby("Cuenta", sort=False):
        chunk = chunk.reset_index(drop=True).copy()
        desc0 = chunk["Descripción"].astype(str).str.strip().str.upper()

        tiene_saldo_inicial = (
            not chunk.empty
            and desc0.iloc[0] == "SALDO INICIAL"
            and float(chunk.loc[0, "Débito"]) == 0.0
            and float(chunk.loc[0, "Crédito"]) == 0.0
        )

        running = float(chunk.loc[0, "Saldo"]) if tiene_saldo_inicial else 0.0
        sal = []

        for i, row in chunk.iterrows():
            if i == 0 and tiene_saldo_inicial:
                sal.append(running)
                continue

            running = running - float(row["Débito"]) + float(row["Crédito"])
            sal.append(running)

        chunk["Saldo"] = sal

        desc = chunk["Descripción"].astype(str).str.strip().str.upper()
        chunk = chunk[
            (desc != "SALDO INICIAL") &
            (~desc.str.contains("SALDO ACTUAL", na=False))
        ].reset_index(drop=True)

        out.append(chunk)

    if not out:
        return df.iloc[0:0].copy()

    return pd.concat(out, ignore_index=True)

def fix_icbc(df):
    return df

def fix_macro(df):
    return df[~df["Descripción"].str.upper().str.startswith("SALDO FINAL")].reset_index(drop=True)

def fix_macro2(df):
    return df

def fix_galicia(df):
    df = df[~df["Descripción"].str.strip().str.upper().str.startswith("TOTAL")].reset_index(drop=True)
    df["Descripción"] = df["Descripción"].str.replace(r"^\d{2}/\d{2}/\d{2}\s*", "", regex=True).str.strip()
    return df

def fix_nacion(df):
    return df[~df["Descripción"].str.upper().str.startswith("SALDO FINAL")].reset_index(drop=True)

def fix_santanderrio(df):
    return df[~df["Descripción"].str.upper().str.startswith("SALDO TOTAL")].reset_index(drop=True)

def fix_supervielle(df):
    if "Cuenta" not in df.columns:
        m = df["Descripción"].str.extract(r"^\[(CTA [^\]]+)\]\s*(.*)$")
        if not m.empty and m[0].notna().any():
            df = df.copy()
            df["Cuenta"] = m[0].fillna("GENERAL").astype(str)
            df["Descripción"] = df["Descripción"].where(m[1].isna(), m[1])
    return df

def fix_supervielle2(df):
    return df

def fix_mp(df):
    return df

def fix_credicoop(df):
    return df

def fix_bbva(df):
    return df  # NO se usa (BBVA sale tal cual del parser individual)

def fix_brubank(df):
    return df

PARSERS: Dict[str, Callable[[bytes], pd.DataFrame]] = {
    "BANCOR": parse_bancor,
    "GALICIA": parse_galicia,
    "ICBC": parse_icbc,
    "PATAGONIA": parse_patagonia,
    "MACRO": parse_macro,
    "MACRO 2": parse_macro2,
    "NACION": parse_nacion,
    "SANTANDER RIO": parse_santanderrio,
    "SUPERVIELLE": parse_supervielle,
    "SUPERVIELLE 2": parse_supervielle2,
    "MERCADO PAGO": parse_mp,
    "CREDICOOP": parse_credicoop,
    "BBVA": parse_bbva,
    "BRUBANK": parse_brubank,
}

FIXES: Dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
    "BANCOR": fix_bancor,
    "GALICIA": fix_galicia,
    "ICBC": fix_icbc,
    "PATAGONIA": fix_patagonia,
    "MACRO": fix_macro,
    "MACRO 2": fix_macro2,
    "NACION": fix_nacion,
    "SANTANDER RIO": fix_santanderrio,
    "SUPERVIELLE": fix_supervielle,
    "SUPERVIELLE 2": fix_supervielle2,
    "MERCADO PAGO": fix_mp,
    "CREDICOOP": fix_credicoop,
    "BBVA": fix_bbva,
    "BRUBANK": fix_brubank,
}

MULTISHEET_BANKS = {"SUPERVIELLE", "BBVA", "PATAGONIA", "BRUBANK"}  # multi-hoja por cuenta

# ===== UI principal =====
card = st.container()
card.markdown('<div class="card">', unsafe_allow_html=True)

col1, col2 = st.columns([1, 2], vertical_alignment="center")
with col1:
    bank_options = ["AUTO"] + sorted(PARSERS.keys())
    bank = st.selectbox("Banco", bank_options, index=0)
with col2:
    st.caption("La salida se formatea como **Fecha | Descripción | Clasificacion | Débito | Crédito | Saldo** (numérico).")

files = st.file_uploader(
    "Subí uno o varios PDF del banco seleccionado",
    type=["pdf"],
    accept_multiple_files=True,
    max_upload_size=80,
)
MAX_FILES = 20
MAX_TOTAL_MB = 300
MAX_FILE_MB_APP = 80

if files:
    if len(files) > MAX_FILES:
        st.error(f"Máximo {MAX_FILES} archivos por carga.")
        st.stop()

    total_mb = sum((f.size or 0) for f in files) / (1024 * 1024)
    if total_mb > MAX_TOTAL_MB:
        st.error(f"El total de archivos supera {MAX_TOTAL_MB} MB. Subí menos archivos por tanda.")
        st.stop()

    too_big = []
    for f in files:
        file_mb = (f.size or 0) / (1024 * 1024)
        if file_mb > MAX_FILE_MB_APP:
            too_big.append(f"{f.name} ({file_mb:.1f} MB)")

    if too_big:
        st.error(
            "Estos archivos superan el límite permitido:\n\n- " + "\n- ".join(too_big)
        )
        st.stop()

add_header = st.checkbox("Encabezado con nombre de archivo", value=True)
add_blank = st.checkbox("Línea en blanco entre archivos", value=True)
do_classification = st.checkbox("Hacer Clasificacion", value=True)
with st.expander("Opciones avanzadas", expanded=False):
    long_mode = st.toggle(
        "Modo LARGO (100+ páginas / miles de líneas)",
        value=True,
        help="Usa timeouts mayores, procesa secuencialmente y prioriza estabilidad en PDFs muy grandes."
    )
    atomic_mode = st.toggle(
        "Procesamiento atómico (si falla uno, no generar Excel)",
        value=True,
        help="Evita períodos salteados: si un archivo falla, se cancela toda la salida."
    )

do_convert = st.button("🔄 Convertir a Excel", type="primary", use_container_width=True)
card.markdown('</div>', unsafe_allow_html=True)

tab_prev, tab_log = st.tabs(["Vista previa", "Registro"])

logs: List[str] = []

def _log(msg: str):
    logs.append(msg)

def _sort_rows_by_fecha(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_i"] = range(len(df))
    return df.sort_values(["Fecha", "_i"]).drop(columns="_i").reset_index(drop=True)

def _append_blocks(chunks: List[pd.DataFrame]):
    if not chunks:
        return pd.DataFrame(columns=EXPECTED_COLS)
    return pd.concat(chunks, ignore_index=True)

if do_convert:
    if not files:
        st.warning("Subí al menos un PDF.")
    else:
        inputs: List[Tuple[str, bytes, str]] = []
        for f in files:
            data = f.read()
            inputs.append((f.name, data, _bytes_hash(data)))
        inputs.sort(key=lambda x: x[0])

        if long_mode:
            timeouts = {
                "BANCOR": 600,
                "GALICIA": 480,
                "ICBC": 480,
                "PATAGONIA": 420,
                "MACRO": 600,
                "MACRO 2": 600,
                "NACION": 480,
                "SANTANDER RIO": 420,
                "SUPERVIELLE": 600,
                "SUPERVIELLE 2": 600,
                "MERCADO PAGO": 480,
                "CREDICOOP": 480,
                "BBVA": 600,
                "BRUBANK": 480,
            }
            max_workers = 1
        else:
            timeouts = {
                "BANCOR": 300,
                "GALICIA": 240,
                "ICBC": 240,
                "PATAGONIA": 240,
                "MACRO": 300,
                "MACRO 2": 300,
                "NACION": 240,
                "SANTANDER RIO": 240,
                "SUPERVIELLE": 300,
                "SUPERVIELLE 2": 300,
                "MERCADO PAGO": 240,
                "CREDICOOP": 240,
                "BBVA": 300,
                "BRUBANK": 240,
            }
            max_workers = min(4, len(inputs))

        def process_one(name: str, data: bytes, timeout_sec: int):
            t0 = time.time()

            detected_bank = bank
            if bank == "AUTO":
                detected_bank = detect_bank(data)
                if not detected_bank:
                    raise ValueError(f"No se pudo detectar automáticamente el banco de {name}")

            with st.spinner(f"Procesando {name} para {detected_bank}…"):
                raw = _run_with_timeout(
                    _cached_parse,
                    detected_bank,
                    _bytes_hash(data),
                    data,
                    timeout=timeout_sec,
                )

                if detected_bank == "BBVA":
                    if isinstance(raw, pd.DataFrame):
                        fin = raw.copy()
                    else:
                        try:
                            fin = pd.DataFrame(raw)
                        except Exception:
                            fin = pd.DataFrame(columns=EXPECTED_COLS)

                    fin = _ensure_columns_for_export(fin)
                    if do_classification:
                        fin = _apply_classification(fin, detected_bank)
                else:
                    mid = _normalize_df(raw)
                    fin = _normalize_df(FIXES[detected_bank](mid))
                    if do_classification:
                        fin = _apply_classification(fin, detected_bank)

                    if detected_bank != "SUPERVIELLE 2":
                        fin = _sort_rows_by_fecha(fin)

            _log(f"✓ {name}: {detected_bank} | {len(fin)} filas en {time.time() - t0:.1f}s")
            return fin, detected_bank

        items = []
        errors = []

        if max_workers == 1:
            for name, data, _h in inputs:
                try:
                    if bank == "AUTO":
                        provisional_bank = detect_bank(data)
                        if not provisional_bank:
                            raise ValueError(f"No se pudo detectar automáticamente el banco de {name}")
                        timeout_sec = timeouts.get(provisional_bank, 300 if not long_mode else 600)
                    else:
                        timeout_sec = timeouts.get(bank, 300 if not long_mode else 600)

                    fin, detected_bank = process_one(name, data, timeout_sec)
                    mind = _df_min_date(fin)
                    items.append((name, fin, mind, detected_bank))
                except TimeoutError:
                    msg = f"{name}: tiempo de procesamiento excedido"
                    errors.append(msg)
                    _log(f"✗ {msg}")
                    if atomic_mode:
                        items = []
                        break
                except Exception as e:
                    msg = f"{name}: {e}"
                    errors.append(msg)
                    _log(f"✗ {msg}")
                    if atomic_mode:
                        items = []
                        break
        else:
            from concurrent.futures import TimeoutError as TE

            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                fut_map = {}

                for name, data, _ in inputs:
                    if bank == "AUTO":
                        provisional_bank = detect_bank(data)
                        if not provisional_bank:
                            errors.append(f"{name}: No se pudo detectar automáticamente el banco")
                            _log(f"✗ {name}: No se pudo detectar automáticamente el banco")
                            if atomic_mode:
                                break
                            continue
                        timeout_sec = timeouts.get(provisional_bank, 300 if not long_mode else 600)
                    else:
                        timeout_sec = timeouts.get(bank, 300 if not long_mode else 600)

                    fut = pool.submit(process_one, name, data, timeout_sec)
                    fut_map[fut] = (name, data)

                tmp = {}
                failed = False

                for fut in as_completed(fut_map):
                    name, data = fut_map[fut]
                    try:
                        tmp[name] = fut.result()
                    except TE:
                        msg = f"{name}: tiempo de procesamiento excedido"
                        errors.append(msg)
                        _log(f"✗ {msg}")
                        if atomic_mode:
                            failed = True
                            break
                    except Exception as e:
                        msg = f"{name}: {e}"
                        errors.append(msg)
                        _log(f"✗ {msg}")
                        if atomic_mode:
                            failed = True
                            break

            if not (atomic_mode and failed):
                for name, _data, _ in inputs:
                    if name in tmp:
                        fin, detected_bank = tmp[name]
                        mind = _df_min_date(fin)
                        items.append((name, fin, mind, detected_bank))

        if errors and atomic_mode:
            tab_log.error("Se canceló la generación del Excel (modo atómico activo).")
            for e in errors:
                tab_log.write("• " + e)

        elif items:
            sortable = []
            for name, fin, mind, detected_bank in items:
                if pd.isna(mind):
                    y, m = _infer_period_from_filename(name)
                    if y and m:
                        mind = pd.Timestamp(year=y, month=m, day=1)
                sortable.append((name, fin, mind, detected_bank))

            def _key(t):
                name, fin, mind, detected_bank = t
                has = 0 if (isinstance(mind, pd.Timestamp) and not pd.isna(mind)) else 1
                return (has, mind if not pd.isna(mind) else pd.Timestamp.max, name.lower())

            sortable.sort(key=_key)

            if bank == "AUTO":
                detected_banks = sorted(set(db for _, _, _, db in sortable))
                if len(detected_banks) > 1:
                    st.error(
                        "Se detectaron múltiples bancos en la misma carga: "
                        + ", ".join(detected_banks)
                        + ". En esta versión, cargá juntos solo PDFs del mismo banco."
                    )
                    st.stop()

            effective_bank = sortable[0][3] if bank == "AUTO" else bank

            if effective_bank in MULTISHEET_BANKS:
                account_map: Dict[str, List[pd.DataFrame]] = {}

                for name, fin, _mind, _detected_bank in sortable:
                    if add_header:
                        header = pd.DataFrame(
                            [[pd.NaT, f"=== {name} ({effective_bank}) ===", "", 0.0, 0.0, 0.0]],
                            columns=EXPECTED_COLS,
                        )
                        cuentas = (
                            fin["Cuenta"].dropna().unique().tolist()
                            if "Cuenta" in fin.columns
                            else ["GENERAL"]
                        )
                        for cta in sorted(set(cuentas)):
                            account_map.setdefault(str(cta), []).append(header)

                    if "Cuenta" in fin.columns:
                        for cta, chunk in fin.groupby(fin["Cuenta"].fillna("GENERAL"), sort=False):
                            chunk = chunk.drop(columns=[c for c in ["Cuenta"] if c in chunk.columns])
                            account_map.setdefault(str(cta), []).append(chunk[EXPECTED_COLS])
                    else:
                        account_map.setdefault("GENERAL", []).append(fin[EXPECTED_COLS])

                    if add_blank:
                        blank = pd.DataFrame(
                            [[pd.NaT, "", "", 0.0, 0.0, 0.0]],
                            columns=EXPECTED_COLS,
                        )
                        for cta in list(account_map.keys()):
                            account_map[cta].append(blank)

                first_sheet = next(iter(account_map))
                result_preview = _append_blocks(account_map[first_sheet])

                with tab_prev:
                    st.subheader(f"Vista previa (hoja: {first_sheet})")
                    st.dataframe(result_preview, use_container_width=True, height=480)

                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine="xlsxwriter", datetime_format="dd/mm/yyyy") as writer:
                    for cta in sorted(account_map.keys()):
                        df_sheet = _append_blocks(account_map[cta])
                        sheet_name = str(cta)[:31]
                        df_sheet.to_excel(writer, index=False, sheet_name=sheet_name)

                        wb = writer.book
                        ws = writer.sheets[sheet_name]

                        if effective_bank == "SUPERVIELLE 2":
                            ws.set_column("A:A", 18, wb.add_format({"num_format": "dd/mm/yyyy hh:mm"}))
                        else:
                            ws.set_column("A:A", 12, wb.add_format({"num_format": "dd/mm/yyyy"}))

                        ws.set_column("B:B", 90)
                        ws.set_column("C:C", 28)
                        ws.set_column("D:F", 16, wb.add_format({"num_format": "0.00"}))

                with tab_prev:
                    st.download_button(
                        "⬇️ Descargar Excel (NUMÉRICO, múltiples hojas por cuenta)",
                        data=buf.getvalue(),
                        file_name=f"EXTRACTOS_{effective_bank}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )

            else:
                chunks = []

                for name, fin, _mind, _detected_bank in sortable:
                    if add_header:
                        chunks.append(
                            pd.DataFrame(
                                [[pd.NaT, f"=== {name} ({effective_bank}) ===", "", 0.0, 0.0, 0.0]],
                                columns=EXPECTED_COLS,
                            )
                        )

                    chunks.append(fin[EXPECTED_COLS])

                    if add_blank:
                        chunks.append(
                            pd.DataFrame(
                                [[pd.NaT, "", "", 0.0, 0.0, 0.0]],
                                columns=EXPECTED_COLS,
                            )
                        )

                result = _append_blocks(chunks)

                with tab_prev:
                    st.subheader("Vista previa")
                    st.dataframe(result, use_container_width=True, height=480)

                    buf = io.BytesIO()
                    with pd.ExcelWriter(buf, engine="xlsxwriter", datetime_format="dd/mm/yyyy") as writer:
                        result.to_excel(writer, index=False, sheet_name="Extractos")

                        wb = writer.book
                        ws = writer.sheets["Extractos"]

                        if effective_bank == "SUPERVIELLE 2":
                            ws.set_column("A:A", 18, wb.add_format({"num_format": "dd/mm/yyyy hh:mm"}))
                        else:
                            ws.set_column("A:A", 12, wb.add_format({"num_format": "dd/mm/yyyy"}))

                        ws.set_column("B:B", 90)
                        ws.set_column("C:C", 28)
                        ws.set_column("D:F", 16, wb.add_format({"num_format": "0.00"}))

                    st.download_button(
                        "⬇️ Descargar Excel (NUMÉRICO)",
                        data=buf.getvalue(),
                        file_name=f"EXTRACTOS_{effective_bank}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )

        with tab_log:
            if logs:
                st.subheader("Registro")
                for line in logs:
                    st.write(line)

            if errors:
                st.error("Archivos con error:")
                for e in errors:
                    st.write("• " + e)

st.markdown("""
<div class="author">
  <div>
    <div class="tiny">autor:</div>
    <div class="name">Tadeo Balfagon</div>
  </div>
</div>
""", unsafe_allow_html=True)
