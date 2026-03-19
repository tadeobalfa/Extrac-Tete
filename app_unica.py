# app_unica.py — Extrac-Tete · Convertidor de Extractos Bancarios
# Ejecutar: streamlit run app_unica.py

import io
import re
import time
import hashlib
import unicodedata
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
from parsers.nacion2 import parse_pdf as parse_nacion2
from parsers.patagonia import parse_pdf as parse_patagonia
from parsers.santanderrio import parse_pdf as parse_santanderrio
from parsers.supervielle import parse_pdf as parse_supervielle
from parsers.supervielle2 import parse_pdf as parse_supervielle2
from parsers.mercadopago import parse_pdf as parse_mp
from parsers.credicoop import parse_pdf as parse_credicoop
from parsers.bbva import parse_pdf as parse_bbva
from parsers.brubank import parse_pdf as parse_brubank

# ====== UI ======
st.set_page_config(
    page_title="Extrac-Tete · Convertidor de Extractos",
    page_icon="💳",
    layout="wide",
)

st.markdown("""
<style>
label {
    color: #f2eaff !important;
    font-weight: 500;
}

.summary-inline {
    display:flex;
    flex-wrap:wrap;
    gap:18px;
    align-items:center;
    justify-content:space-between;
    padding:10px 14px;
    margin-bottom:10px;
    border:1px solid rgba(255,255,255,.08);
    background: rgba(255,255,255,.04);
    border-radius: 12px;
    font-size:14px;
    color:#e9dff9;
}

.summary-inline strong {
    color:#ffffff;
}

.summary-inline div {
    opacity:.95;
}

.summary-status-ok {
    color:#7ff0a5;
    font-weight:700;
}

.summary-status-review {
    color:#ffc857;
    font-weight:700;
}

.validation-card {
    margin-top: 16px;
    margin-bottom: 18px;
    border: 1px solid rgba(255,255,255,.08);
    background: rgba(255,255,255,.04);
    border-radius: 18px;
    overflow: hidden;
    box-shadow: 0 10px 30px rgba(0,0,0,.28);
}

.validation-title {
    font-size: 22px;
    font-weight: 800;
    color: #f4eaff;
    padding: 18px 20px;
    border-bottom: 1px solid rgba(255,255,255,.08);
}

.validation-summary {
    padding: 18px 20px 12px 20px;
}

.validation-status {
    font-size: 18px;
    margin-bottom: 10px;
    color: #f2eaff;
}

.validation-bullets {
    margin: 0;
    padding-left: 22px;
    color: #d9c8f8;
    font-size: 16px;
    line-height: 1.7;
}

.status-green {
    color: #7ff0a5;
    font-weight: 800;
}

.status-yellow {
    color: #ffc857;
    font-weight: 800;
}

.validation-table-wrap {
    border: 1px solid rgba(255,255,255,.08);
    background: rgba(255,255,255,.03);
    border-radius: 16px;
    overflow: hidden;
    margin-bottom: 22px;
}

.validation-table {
    width: 100%;
    border-collapse: collapse;
    color: #f2eaff;
    font-size: 15px;
}

.validation-table thead th {
    text-align: left;
    padding: 14px 16px;
    background: rgba(255,255,255,.03);
    border-bottom: 1px solid rgba(255,255,255,.08);
    font-size: 15px;
}

.validation-table tbody td {
    padding: 14px 16px;
    border-top: 1px solid rgba(255,255,255,.06);
    vertical-align: top;
}

.alert-badge {
    display: inline-block;
    padding: 6px 12px;
    border-radius: 10px;
    font-weight: 800;
    font-size: 14px;
    line-height: 1;
    white-space: nowrap;
}

.alert-critical {
    background: linear-gradient(90deg,#ff4d5a,#ff6a3d);
    color: white;
}

.alert-medium {
    background: linear-gradient(90deg,#ffb020,#ffcf4d);
    color: #342100;
}

.alert-info {
    background: linear-gradient(90deg,#f8df6b,#fff08b);
    color: #322600;
}

.stCheckbox label,
.stCheckbox p,
.stSelectbox label,
[data-testid="stFileUploader"] label {
    color: #f2eaff !important;
}

[data-testid="stMarkdownContainer"] p {
    color: #f2eaff !important;
}

header {visibility: hidden;}
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

[data-testid="stHeader"] {
    background: transparent !important;
}

[data-testid="stToolbar"] {
    display: none !important;
}

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

.note-wrap {
    display:flex;
    align-items:flex-start;
    gap:10px;
    margin-bottom:14px;
}

.h-note {
    font-size:14px;
    opacity:.92;
    color:#e9dff9;
}

.help-box {
    display:inline-block;
}

.help-box details {
    position: relative;
}

.help-box summary {
    list-style:none;
    cursor:pointer;
    width:24px;
    height:24px;
    border-radius:999px;
    border:1px solid rgba(255,255,255,.16);
    background: rgba(255,255,255,.06);
    color:#f2eaff;
    display:flex;
    align-items:center;
    justify-content:center;
    font-weight:800;
    user-select:none;
}

.help-box summary::-webkit-details-marker {
    display:none;
}

.help-content {
    position:absolute;
    top:32px;
    right:0;
    width:420px;
    max-width: min(420px, 80vw);
    z-index:99;
    background: #1b1030;
    border:1px solid rgba(255,255,255,.10);
    border-radius:14px;
    padding:14px 16px;
    box-shadow: 0 10px 30px rgba(0,0,0,.35);
    color:#f2eaff;
    font-size:14px;
    line-height:1.5;
}

.help-content strong {
    color:#ffffff;
}

.card {
    border:1px solid var(--stroke);
    background: var(--card);
    border-radius: 16px;
    padding: 18px 16px 8px 16px;
    box-shadow: 0 10px 30px rgba(0,0,0,.35), inset 0 0 0 1px rgba(255,255,255,.02);
}

.stSelectbox > div > div,
.stFileUploader > div {
    background: rgba(255,255,255,.06);
    border: 1px solid rgba(255,255,255,.1);
    border-radius: 12px;
}

.stSelectbox div[data-baseweb="select"] {
    background: rgba(255,255,255,0.06) !important;
    border-radius: 12px !important;
}

.stSelectbox div[data-baseweb="select"] > div {
    color: #f2eaff !important;
}

.stSelectbox svg {
    fill: #cbb6f3 !important;
}

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

button[kind="secondary"] {
    background: rgba(255,255,255,0.08) !important;
    color: #ffffff !important;
    border: 1px solid rgba(255,255,255,0.15) !important;
}

button[kind="secondary"]:hover {
    background: rgba(255,255,255,0.15) !important;
}

[data-testid="stFileUploaderDropzone"] button {
    background: rgba(255,255,255,0.08) !important;
    color: #ffffff !important;
    border: 1px solid rgba(255,255,255,0.15) !important;
}

button span {
    color: inherit !important;
}

.stCheckbox > label,
.stCheckbox > div > label {
    color: var(--ink) !important;
}

input[type="checkbox"] {
    accent-color: var(--accent3);
}

.stTabs [data-baseweb="tab"] {
    color: var(--muted);
    font-weight: 600;
}

.stTabs [data-baseweb="tab"][aria-selected="true"] {
    color: var(--ink);
    border-bottom: 2px solid var(--accent1);
}

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

.stDownloadButton button {
    background: linear-gradient(90deg,#ff3ea5,#ff7a5f 45%,#ffc53d 95%) !important;
    color: #1c102d !important;
    font-weight: 700 !important;
    border-radius: 12px !important;
    border: none !important;
}

.stDownloadButton button:hover {
    filter: brightness(1.05);
}

.stDataFrame {
    border-radius: 12px;
    overflow: hidden;
}

.progress-wrap {
    margin: 10px 0 18px 0;
    padding: 12px 14px;
    border: 1px solid rgba(255,255,255,.08);
    background: rgba(255,255,255,.04);
    border-radius: 14px;
}

.progress-label {
    display:flex;
    justify-content:space-between;
    align-items:center;
    font-size:14px;
    color:#f2eaff;
    margin-bottom:8px;
}

.progress-track {
    width:100%;
    height:12px;
    background: rgba(255,255,255,.08);
    border-radius:999px;
    overflow:hidden;
    box-shadow: inset 0 1px 2px rgba(0,0,0,.25);
}

.progress-fill {
    height:100%;
    width:0%;
    border-radius:999px;
    background: linear-gradient(90deg,#ff3ea5,#ff7a5f 45%,#ffc53d 95%);
    transition: width .25s ease;
    box-shadow: 0 0 14px rgba(255,62,165,.35);
}

.progress-sub {
    margin-top:8px;
    font-size:13px;
    color:#cbb6f3;
    opacity:.95;
}

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

<div class="note-wrap">
    <div class="h-note">
        Elegí el banco, cargá uno o más archivos PDF y convertí el extracto a Excel en pocos pasos.
    </div>
    <div class="help-box">
        <details>
            <summary>?</summary>
            <div class="help-content">
                <strong>Cómo procesar un extracto bancario</strong><br><br>
                1. <strong>Seleccioná el banco</strong> que querés procesar. También podés usar <strong>AUTO</strong> para que la app intente detectarlo automáticamente.<br><br>
                2. <strong>Cargá uno o varios archivos PDF</strong> del mismo banco.<br><br>
                3. Elegí las opciones que quieras usar:<br>
                • <strong>Línea en blanco entre archivos</strong>: inserta una fila vacía entre un archivo y otro cuando procesás más de un PDF.<br>
                • <strong>Hacer Clasificacion</strong>: intenta clasificar automáticamente los movimientos del extracto de forma aproximada.<br><br>
                4. Presioná <strong>Convertir a Excel</strong> y esperá a que finalice el procesamiento.<br><br>
                5. Cuando termine, en la parte inferior aparecerá el botón <strong>Descargar Excel (NUMÉRICO)</strong> o su variante multihoja, según el banco procesado.
            </div>
        </details>
    </div>
</div>
""", unsafe_allow_html=True)

# ===== Utilidades =====
EXPECTED_COLS = ["Fecha", "Descripción", "Clasificacion", "Débito", "Crédito", "Saldo"]

CLASSIF_FILE_CANDIDATES = [
    Path(__file__).resolve().parent / "CLASIFICACION EXTRACTOS.xlsx",
    Path("/mnt/data/CLASIFICACION EXTRACTOS.xlsx"),
]

FORMAT_IMG_DIR_CANDIDATES = [
    Path(__file__).resolve().parent / "formatos_pdf",
    Path("/mnt/data/formatos_pdf"),
]

BANK_FORMAT_IMAGES = {
    "GALICIA": "galicia.png",
    "SUPERVIELLE": "supervielle.png",
    "SUPERVIELLE 2": "supervielle2.png",
    "BANCOR": "bancor.png",
    "NACION": "nacion.png",
    "NACION 2": "nacion2.png",
    "MERCADO PAGO": "mercadopago.png",
    "MACRO": "macro.png",
    "MACRO 2": "macro2.png",
    "CREDICOOP": "credicoop.png",
    "BBVA": "bbva.png",
    "ICBC": "icbc.png",
    "PATAGONIA": "patagonia.png",
    "SANTANDER RIO": "santander.png",
    "BRUBANK": "brubank.png",
}

FORMAT_NOTE = """
**Formato requerido del PDF**

El archivo debe ser el **extracto original descargado del banco**.

**No se pueden subir:**
- capturas de pantalla
- PDFs escaneados
- PDFs editados o modificados

Si el formato del PDF difiere del ejemplo, el sistema puede no detectar correctamente los movimientos.
"""

def _find_format_image(bank_name: str) -> Optional[Path]:
    fname = BANK_FORMAT_IMAGES.get(bank_name)
    if not fname:
        return None

    for base in FORMAT_IMG_DIR_CANDIDATES:
        p = base / fname
        if p.exists():
            return p

    return None

def _render_bank_format_help(bank_name: str):
    if bank_name == "AUTO":
        with st.expander("ℹ Ver formato esperado del PDF", expanded=False):
            st.info("Seleccioná un banco específico para ver el ejemplo visual del formato esperado.")
        return

    img_path = _find_format_image(bank_name)

    with st.expander("ℹ Ver formato esperado del PDF", expanded=False):
        st.markdown(f"**Ejemplo de extracto válido – {bank_name}**")

        if img_path and img_path.exists():
            st.image(str(img_path), use_container_width=True)
        else:
            st.warning(f"No se encontró la imagen de ejemplo para {bank_name}.")

        st.markdown(FORMAT_NOTE)

BANK_RULE_SHEETS = {
    "BANCOR": "BANCOR",
    "GALICIA": "GALICIA",
    "ICBC": "ICBC",
    "PATAGONIA": "PATAGONIA",
    "MACRO": "MACRO",
    "MACRO 2": "MACRO 2",
    "NACION": "NACION",
    "NACION 2": "NACION",
    "SANTANDER RIO": "SANTANDER RIO",
    "SUPERVIELLE": "SUPERVIELLE",
    "SUPERVIELLE 2": "SUPERVIELLE 2",
    "MERCADO PAGO": "MERCADO PAGO",
    "CREDICOOP": "CREDICOOP",
    "BBVA": "BBVA",
    "BRUBANK": "BRUBANK",
}

BANK_SHEET_ALIASES = {
    "BANCOR": ["BANCOR", "BANCO CORDOBA", "BANCO DE CORDOBA"],
    "GALICIA": ["GALICIA", "BANCO GALICIA"],
    "ICBC": ["ICBC"],
    "PATAGONIA": ["PATAGONIA", "BANCO PATAGONIA"],
    "MACRO": ["MACRO", "BANCO MACRO"],
    "MACRO 2": ["MACRO 2", "MACRO2", "BANCO MACRO 2"],
    "NACION": ["NACION", "BANCO NACION", "BANCO DE LA NACION ARGENTINA"],
    "NACION 2": ["NACION 2", "NACION2", "BANCO NACION 2", "BANCO DE LA NACION ARGENTINA 2"],
    "SANTANDER RIO": ["SANTANDER RIO", "SANTANDER", "SANTANDER RIO "],
    "SUPERVIELLE": ["SUPERVIELLE", "BANCO SUPERVIELLE"],
    "SUPERVIELLE 2": ["SUPERVIELLE 2", "SUPERVIELLE2", "BANCO SUPERVIELLE 2"],
    "MERCADO PAGO": ["MERCADO PAGO", "MERCADOPAGO"],
    "CREDICOOP": ["CREDICOOP", "BANCO CREDICOOP"],
    "BBVA": ["BBVA", "BANCO BBVA"],
    "BRUBANK": ["BRUBANK", "BRU BANK"],
}

BANK_CLASSIFICATION_FALLBACKS = {
    "MACRO 2": ["MACRO 2", "MACRO"],
    "SUPERVIELLE 2": ["SUPERVIELLE 2", "SUPERVIELLE"],
    "BRUBANK": ["BRUBANK"],
    "NACION 2": ["NACION 2", "NACION"],
}

def _norm_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.upper().strip()
    s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = re.sub(r"\s+", " ", s)
    return s

def _find_classif_file() -> Optional[Path]:
    for p in CLASSIF_FILE_CANDIDATES:
        if p.exists():
            return p
    return None

def _resolve_sheet_name(xls: pd.ExcelFile, bank_name: str) -> Optional[str]:
    sheet_map = {_norm_text(s): s for s in xls.sheet_names}

    candidates = []
    if bank_name in BANK_RULE_SHEETS:
        candidates.append(BANK_RULE_SHEETS[bank_name])

    candidates.extend(BANK_SHEET_ALIASES.get(bank_name, []))

    for cand in candidates:
        real = sheet_map.get(_norm_text(cand))
        if real:
            return real

    return None

@st.cache_data(show_spinner=False)
def _load_classification_rules(_mtime: Optional[float] = None):
    path = _find_classif_file()
    if path is None:
        return {"BANK": {}, "GENERAL": []}

    xls = pd.ExcelFile(path)

    bank_rules: Dict[str, List[dict]] = {}
    general_rules: List[dict] = []

    for bank_name in BANK_RULE_SHEETS.keys():
        sheet_name = _resolve_sheet_name(xls, bank_name)

        if not sheet_name:
            bank_rules[bank_name] = []
            continue

        df_sheet = pd.read_excel(path, sheet_name=sheet_name)
        df_sheet.columns = [str(c).strip() for c in df_sheet.columns]

        desc_col = None
        class_col = None

        for c in df_sheet.columns:
            cu = _norm_text(c)
            if cu in {"DESCRIPCION", "DESCRIPCION BASE", "DESCRIPCION BANCO", "DESCRIPCION ORIGINAL"}:
                desc_col = c
            elif cu in {"CLASIFICACION", "CLASIFICACION FINAL"}:
                class_col = c

        rules = []
        if desc_col and class_col:
            for idx, row in df_sheet.iterrows():
                pattern = _norm_text(row.get(desc_col, ""))
                clasif = str(row.get(class_col, "")).strip()

                if not pattern or pattern == "NAN" or not clasif or _norm_text(clasif) == "NAN":
                    continue

                rules.append({
                    "pattern": pattern,
                    "clasificacion": clasif,
                    "priority_len": len(pattern),
                    "order": idx,
                })

        bank_rules[bank_name] = rules

    general_sheet_name = None
    for s in xls.sheet_names:
        if _norm_text(s) == "GENERAL":
            general_sheet_name = s
            break

    if general_sheet_name:
        df_gen = pd.read_excel(path, sheet_name=general_sheet_name)
        df_gen.columns = [str(c).strip() for c in df_gen.columns]

        desc_col = None
        dc_col = None
        class_col = None

        for c in df_gen.columns:
            cu = _norm_text(c)
            if cu in {"DESCRIPCION", "DESCRIPCION BASE", "DESCRIPCION BANCO", "DESCRIPCION ORIGINAL"}:
                desc_col = c
            elif cu in {"DEBITO/CREDITO", "DEBITO CREDITO", "TIPO"}:
                dc_col = c
            elif cu in {"CLASIFICACION", "CLASIFICACION FINAL"}:
                class_col = c

        if desc_col and class_col:
            for idx, row in df_gen.iterrows():
                pattern = _norm_text(row.get(desc_col, ""))
                dc = _norm_text(row.get(dc_col, "")) if dc_col else ""
                clasif = str(row.get(class_col, "")).strip()

                if not pattern or pattern == "NAN" or not clasif or _norm_text(clasif) == "NAN":
                    continue

                general_rules.append({
                    "pattern": pattern,
                    "debcred": dc,
                    "clasificacion": clasif,
                    "priority_len": len(pattern),
                    "order": idx,
                })

    return {"BANK": bank_rules, "GENERAL": general_rules}

def _get_bank_rules_with_fallback(bank: str, rules_pack: dict) -> List[dict]:
    banks_to_try = BANK_CLASSIFICATION_FALLBACKS.get(bank, [bank])

    merged = []
    seen = set()

    for b in banks_to_try:
        for rule in rules_pack["BANK"].get(b, []):
            key = (rule["pattern"], rule["clasificacion"])
            if key not in seen:
                seen.add(key)
                merged.append(rule)

    return merged

def _classify_row(bank: str, descripcion: str, debito: float, credito: float, rules_pack: dict) -> str:
    desc = _norm_text(descripcion)
    if not desc:
        return ""

    bank_candidates = []
    bank_rules = _get_bank_rules_with_fallback(bank, rules_pack)

    for rule in bank_rules:
        if rule["pattern"] and rule["pattern"] in desc:
            bank_candidates.append(rule)

    if bank_candidates:
        bank_candidates = sorted(bank_candidates, key=lambda r: (-r["priority_len"], r["order"]))
        return bank_candidates[0]["clasificacion"]

    general_candidates = []
    for rule in rules_pack["GENERAL"]:
        if not rule["pattern"] or rule["pattern"] not in desc:
            continue

        cond = _norm_text(rule.get("debcred", ""))

        if cond == "DEBITO" and float(debito) <= 0:
            continue
        if cond == "CREDITO" and float(credito) <= 0:
            continue

        general_candidates.append(rule)

    if general_candidates:
        general_candidates = sorted(general_candidates, key=lambda r: (-r["priority_len"], r["order"]))
        return general_candidates[0]["clasificacion"]

    return "SIN CLASIFICAR"

def _apply_classification(df: pd.DataFrame, bank: str) -> pd.DataFrame:
    df = df.copy()

    if "Clasificacion" not in df.columns:
        df["Clasificacion"] = ""

    path = _find_classif_file()
    mtime = path.stat().st_mtime if path is not None else None
    rules_pack = _load_classification_rules(mtime)

    bank_rules_count = len(_get_bank_rules_with_fallback(bank, rules_pack))
    general_rules_count = len(rules_pack.get("GENERAL", []))
    _log(f"Clasificación {bank}: {bank_rules_count} reglas banco | {general_rules_count} reglas generales")

    if bank_rules_count == 0 and general_rules_count == 0:
        _log(f"Advertencia: no se encontraron reglas de clasificación para {bank}")

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

def _ensure_columns_for_export(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    rename = {}
    for col in df.columns:
        low = str(col).strip().lower()
        if low in {"descripcion completa", "descripción completa"}:
            rename[col] = "Descripción"
        elif low in {"debito", "débito"}:
            rename[col] = "Débito"
        elif low in {"credito", "crédito"}:
            rename[col] = "Crédito"
        elif low == "saldo":
            rename[col] = "Saldo"
        elif low == "fecha":
            rename[col] = "Fecha"
        elif low == "cuenta":
            rename[col] = "Cuenta"

    if rename:
        df = df.rename(columns=rename)

    for c in EXPECTED_COLS:
        if c not in df.columns:
            if c in {"Débito", "Crédito", "Saldo"}:
                df[c] = 0.0
            else:
                df[c] = ""

    if "Cuenta" in df.columns:
        return df[EXPECTED_COLS + ["Cuenta"]]

    return df[EXPECTED_COLS]

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
            (desc != "SALDO INICIAL")
            & (~desc.str.contains("SALDO ACTUAL", na=False))
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

def fix_nacion2(df):
    return df

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
    df = df.copy()

    if df.empty:
        return df

    # asegurar columnas
    for c in ["Fecha", "Descripción", "Débito", "Crédito", "Saldo"]:
        if c not in df.columns:
            if c == "Fecha":
                df[c] = pd.NaT
            elif c == "Descripción":
                df[c] = ""
            else:
                df[c] = 0.0

    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df["Descripción"] = df["Descripción"].astype(str).fillna("")
    df["Débito"] = df["Débito"].apply(_coerce_number)
    df["Crédito"] = df["Crédito"].apply(_coerce_number)
    df["Saldo"] = df["Saldo"].apply(_coerce_number)

    if "Cuenta" not in df.columns:
        return df

    # cuenta principal: NO tocar su lógica actual
    target_account = "CC $ 084-335800-9"

    trailing_amt_re = re.compile(
        r"^(.*?)([-+]?\s*\$?\s*(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d{2})(?:-)?)\s*$"
    )

    aux_markers = (
        "FECHA DE EMISION FECHA DE PAGO NRO DE CHEQUE",
        "FECHA DE EMISIÓN FECHA DE PAGO NRO DE CHEQUE",
        "MOVIMIENTOS PENDIENTES DE DEBITAR",
        "TRANSFERENCIAS",
        "RECIBIDAS (INFORMACION AL",
        "RECIBIDAS (INFORMACIÓN AL",
        "CUENTA ORIGEN",
        "NRO DE CHEQUE",
        "INFORMACION AL FECHA DE EMISION",
        "INFORMACIÓN AL FECHA DE EMISIÓN",
        "CHEQUES ELECTRONICOS INFORMACION AL",
        "CHEQUES ELECTRÓNICOS INFORMACIÓN AL",
        "LEGALES Y AVISOS",
        "IMPUESTO A LOS DEBITOS Y CREDITOS",
        "IMPUESTO A LOS DÉBITOS Y CRÉDITOS",
        "REGIMEN SISTEMA SIRCREB",
        "RÉGIMEN SISTEMA SIRCREB",
        "TOTAL COBRADO",
        "TOTAL DEV.",
        "IMP. NETO",
        "EL CREDITO DE IMPUESTO SUSCEPTIBLE",
        "EL CRÉDITO DE IMPUESTO SUSCEPTIBLE",
        "PERIODO AL TOTAL DEL IMPORTE DEBITADO",
        "VER LEGALES: MOVIMIENTOS CTA.CTE.BANCARIA",
        "VER LEGALES: MOVIMIENTOS CTA CTE BANCARIA",
        "MOVIMIENTOS CTA.CTE.BANCARIA",
        "MOVIMIENTOS CTA CTE BANCARIA",
    )

    def _is_noise_desc(desc: str) -> bool:
        up = str(desc).upper().strip()
        if not up:
            return True
        return any(m in up for m in aux_markers)

    def _looks_like_real_movement(fecha, desc: str, deb: float, cred: float, saldo: float) -> bool:
        up = str(desc).upper().strip()

        if "SIN MOVIMIENTOS" in up:
            return False
        if _is_noise_desc(desc):
            return False

        # fila con fecha + movimiento o saldo razonable
        if pd.notna(fecha) and (deb != 0.0 or cred != 0.0):
            return True

        # algunos movimientos pueden venir solo con saldo
        if pd.notna(fecha) and saldo != 0.0 and (
            "IMP.LEY" in up
            or "LEY NRO" in up
            or "SIRCREB" in up
            or "TRANSFERENCIA" in up
            or "PAGO" in up
            or "CHEQUE" in up
            or "COMI" in up
            or "IVA" in up
            or "OPER.FONDO" in up
            or "CUENTA VISA" in up
        ):
            return True

        return False

    result_chunks = []

    for cuenta, chunk in df.groupby("Cuenta", sort=False):
        cta = str(cuenta).strip()

        # =====================================================
        # 1) CUENTA PRINCIPAL -> dejar EXACTAMENTE como estaba
        # =====================================================
        if cta == target_account:
            chunk = chunk.reset_index(drop=True).copy()
            fixed_rows = []

            prev_good_saldo = None
            prev_good_date = None
            in_aux_block = False

            for _, row in chunk.iterrows():
                fecha = row["Fecha"]
                desc = str(row["Descripción"]).strip()
                up = desc.upper()
                deb = float(row["Débito"] or 0.0)
                cred = float(row["Crédito"] or 0.0)
                saldo = float(row["Saldo"] or 0.0)

                # -------------------------------------------------
                # 1) Bloques auxiliares que no son movimientos reales
                # -------------------------------------------------
                if any(m in up for m in aux_markers):
                    in_aux_block = True
                    continue

                if in_aux_block:
                    if pd.notna(fecha) and prev_good_date is not None:
                        delta_days = abs((fecha - prev_good_date).days)

                        looks_like_real_movement = (
                            delta_days <= 90
                            and not any(m in up for m in aux_markers)
                            and (
                                "IMP.LEY" in up
                                or "LEY NRO" in up
                                or "SIRCREB" in up
                                or "TRANSFERENCIA" in up
                                or "PAGO" in up
                                or "CHEQUE" in up
                                or deb != 0.0
                                or (cred != 0.0 and saldo != 0.0)
                            )
                        )

                        if looks_like_real_movement:
                            in_aux_block = False
                        else:
                            continue
                    else:
                        continue

                # -------------------------------------------------
                # 2) Lógica histórica BBVA:
                #    saldo viene en Crédito y Saldo queda en 0
                # -------------------------------------------------
                if saldo == 0.0 and cred > 0.0:
                    saldo_real = cred
                    deb_real = 0.0
                    cred_real = 0.0
                    desc_real = desc

                    m = trailing_amt_re.match(desc)

                    # 2.a) Si el importe viene pegado al final de la descripción
                    if m:
                        desc_base = m.group(1).rstrip(" -—•:")
                        amt_txt = m.group(2)
                        amt_val = _coerce_number(amt_txt)

                        desc_real = desc_base

                        if amt_val < 0:
                            deb_real = abs(amt_val)
                            cred_real = 0.0
                        elif amt_val > 0:
                            cred_real = abs(amt_val)
                            deb_real = 0.0

                    # 2.b) Si NO viene importe en descripción, inferir por delta de saldo
                    elif prev_good_saldo is not None:
                        delta = saldo_real - prev_good_saldo

                        if abs(delta) >= 0.004:
                            if delta > 0:
                                cred_real = round(delta, 2)
                                deb_real = 0.0
                            else:
                                deb_real = round(abs(delta), 2)
                                cred_real = 0.0

                    else:
                        prev_good_saldo = saldo_real
                        prev_good_date = fecha if pd.notna(fecha) else prev_good_date

                        fixed_row = row.copy()
                        fixed_row["Descripción"] = desc_real
                        fixed_row["Débito"] = 0.0
                        fixed_row["Crédito"] = 0.0
                        fixed_row["Saldo"] = round(saldo_real, 2)
                        fixed_rows.append(fixed_row)
                        continue

                    desc = desc_real
                    deb = round(deb_real, 2)
                    cred = round(cred_real, 2)
                    saldo = round(saldo_real, 2)

                # -------------------------------------------------
                # 3) Reparación adicional del bloque 204–220:
                #    importe al final de descripción + saldo corrido
                # -------------------------------------------------
                elif prev_good_saldo is not None:
                    m = trailing_amt_re.match(desc)
                    if m:
                        desc_base = m.group(1).rstrip(" -—•:")
                        amt_txt = m.group(2)
                        amt_val = _coerce_number(amt_txt)

                        deb_fix = abs(amt_val) if amt_val < 0 else 0.0
                        cred_fix = abs(amt_val) if amt_val > 0 else 0.0
                        expected_saldo = round(prev_good_saldo - deb_fix + cred_fix, 2)

                        credit_is_expected_saldo = abs(cred - expected_saldo) <= 0.05
                        saldo_is_absurd = abs(saldo) > max(abs(expected_saldo) * 3, 5_000_000)

                        if credit_is_expected_saldo or saldo_is_absurd:
                            desc = desc_base
                            deb = deb_fix
                            cred = cred_fix
                            saldo = expected_saldo

                fixed_row = row.copy()
                fixed_row["Descripción"] = desc
                fixed_row["Débito"] = round(deb, 2)
                fixed_row["Crédito"] = round(cred, 2)
                fixed_row["Saldo"] = round(saldo, 2)

                fixed_rows.append(fixed_row)

                if pd.notna(fecha):
                    prev_good_date = fecha

                prev_good_saldo = float(fixed_row["Saldo"])

            if fixed_rows:
                fixed_chunk = pd.DataFrame(fixed_rows)
            else:
                fixed_chunk = chunk.iloc[0:0].copy()

            result_chunks.append(fixed_chunk)
            continue

        # =====================================================
        # 2) RESTO DE CUENTAS -> limpieza GENÉRICA y CONSERVADORA
        # =====================================================
        chunk = chunk.reset_index(drop=True).copy()

        # detectar si la cuenta realmente tiene movimientos
        real_movement_mask = chunk.apply(
            lambda r: _looks_like_real_movement(
                r["Fecha"],
                r["Descripción"],
                float(r["Débito"] or 0.0),
                float(r["Crédito"] or 0.0),
                float(r["Saldo"] or 0.0),
            ),
            axis=1,
        )

        has_real_movements = bool(real_movement_mask.any())

        cleaned_rows = []
        prev_saldo = None

        for _, row in chunk.iterrows():
            fecha = row["Fecha"]
            desc = str(row["Descripción"]).strip()
            up = desc.upper()
            deb = float(row["Débito"] or 0.0)
            cred = float(row["Crédito"] or 0.0)
            saldo = float(row["Saldo"] or 0.0)

            # si la cuenta NO tiene movimientos reales, dejarla vacía
            if not has_real_movements:
                continue

            # eliminar únicamente ruido / legales / impuestos / headers
            if _is_noise_desc(desc):
                continue
            if "SIN MOVIMIENTOS" in up:
                continue

            # si no tiene fecha ni movimiento, afuera
            if pd.isna(fecha) and deb == 0.0 and cred == 0.0:
                continue

            fixed_row = row.copy()

            # lógica BBVA mínima también para otras cuentas:
            # si saldo quedó en 0 y crédito en realidad era saldo
            if saldo == 0.0 and cred > 0.0:
                saldo_real = cred
                deb_real = 0.0
                cred_real = 0.0
                desc_real = desc

                m = trailing_amt_re.match(desc)

                if m:
                    desc_base = m.group(1).rstrip(" -—•:")
                    amt_txt = m.group(2)
                    amt_val = _coerce_number(amt_txt)

                    desc_real = desc_base

                    if amt_val < 0:
                        deb_real = abs(amt_val)
                        cred_real = 0.0
                    elif amt_val > 0:
                        cred_real = abs(amt_val)
                        deb_real = 0.0

                elif prev_saldo is not None:
                    delta = saldo_real - prev_saldo
                    if abs(delta) >= 0.004:
                        if delta > 0:
                            cred_real = round(delta, 2)
                            deb_real = 0.0
                        else:
                            deb_real = round(abs(delta), 2)
                            cred_real = 0.0

                fixed_row["Descripción"] = desc_real
                fixed_row["Débito"] = round(deb_real, 2)
                fixed_row["Crédito"] = round(cred_real, 2)
                fixed_row["Saldo"] = round(saldo_real, 2)
            else:
                # solo recalcular saldo si claramente vino roto
                if prev_saldo is not None:
                    m = trailing_amt_re.match(desc)
                    if m:
                        desc_base = m.group(1).rstrip(" -—•:")
                        amt_txt = m.group(2)
                        amt_val = _coerce_number(amt_txt)

                        deb_fix = abs(amt_val) if amt_val < 0 else 0.0
                        cred_fix = abs(amt_val) if amt_val > 0 else 0.0
                        expected_saldo = round(prev_saldo - deb_fix + cred_fix, 2)

                        credit_is_expected_saldo = abs(cred - expected_saldo) <= 0.05
                        saldo_is_absurd = abs(saldo) > max(abs(expected_saldo) * 3, 100_000)

                        if credit_is_expected_saldo or saldo_is_absurd:
                            fixed_row["Descripción"] = desc_base
                            fixed_row["Débito"] = round(deb_fix, 2)
                            fixed_row["Crédito"] = round(cred_fix, 2)
                            fixed_row["Saldo"] = round(expected_saldo, 2)

            cleaned_rows.append(fixed_row)
            prev_saldo = float(cleaned_rows[-1]["Saldo"])

        if cleaned_rows:
            fixed_chunk = pd.DataFrame(cleaned_rows)
        else:
            fixed_chunk = chunk.iloc[0:0].copy()

        result_chunks.append(fixed_chunk)

    if not result_chunks:
        return df.iloc[0:0].copy()

    return pd.concat(result_chunks, ignore_index=True)
    
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
    "NACION 2": parse_nacion2,
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
    "NACION 2": fix_nacion2,
    "SANTANDER RIO": fix_santanderrio,
    "SUPERVIELLE": fix_supervielle,
    "SUPERVIELLE 2": fix_supervielle2,
    "MERCADO PAGO": fix_mp,
    "CREDICOOP": fix_credicoop,
    "BBVA": fix_bbva,
    "BRUBANK": fix_brubank,
}

MULTISHEET_BANKS = {"SUPERVIELLE", "BBVA", "PATAGONIA", "BRUBANK"}

# ===== UI principal =====
card = st.container()
card.markdown('<div class="card">', unsafe_allow_html=True)

col1, col2 = st.columns([1, 2], vertical_alignment="center")
with col1:
    bank_options = ["AUTO"] + sorted(PARSERS.keys())
    bank = st.selectbox("Banco", bank_options, index=0)

with col2:
    st.caption("La salida se formatea como **Fecha | Descripción | Clasificacion | Débito | Crédito | Saldo** (numérico).")

_render_bank_format_help(bank)
files = st.file_uploader(
    "Subí uno o varios PDF del banco seleccionado",
    type=["pdf"],
    accept_multiple_files=True,
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
        st.error("Estos archivos superan el límite permitido:\n\n- " + "\n- ".join(too_big))
        st.stop()

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

tab_prev, tab_log, tab_hist = st.tabs(["Vista previa", "Registro", "Historial"])

if "history" not in st.session_state:
    st.session_state.history = []

logs: List[str] = []

def _log(msg: str):
    logs.append(msg)

def _add_history_entry(
    bank_selected: str,
    effective_bank: str,
    files_names: List[str],
    status: str,
    rows_count: int,
    output_name: str,
):
    st.session_state.history.insert(
        0,
        {
            "Fecha": pd.Timestamp.now().strftime("%d/%m/%Y %H:%M"),
            "Banco seleccionado": bank_selected,
            "Banco procesado": effective_bank,
            "Archivos": len(files_names),
            "Archivos cargados": ", ".join(files_names[:5]) + (" ..." if len(files_names) > 5 else ""),
            "Estado": status,
            "Filas": rows_count,
            "Salida": output_name,
        }
    )
    st.session_state.history = st.session_state.history[:20]

def _sort_rows_by_fecha(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_i"] = range(len(df))
    return df.sort_values(["Fecha", "_i"]).drop(columns="_i").reset_index(drop=True)

def _append_blocks(chunks: List[pd.DataFrame]):
    if not chunks:
        return pd.DataFrame(columns=EXPECTED_COLS)
    return pd.concat(chunks, ignore_index=True)

def _render_progress(box, current: int, total: int, label: str, sublabel: str = ""):
    pct = 0 if total <= 0 else int((current / total) * 100)
    box.markdown(
        f"""
        <div class="progress-wrap">
            <div class="progress-label">
                <span>{label}</span>
                <span>{pct}%</span>
            </div>
            <div class="progress-track">
                <div class="progress-fill" style="width:{pct}%;"></div>
            </div>
            <div class="progress-sub">{sublabel}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def _count_pdf_pages(raw_bytes: bytes) -> int:
    try:
        with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
            return len(pdf.pages)
    except Exception:
        return 0


def _is_blank_visual_row(row: pd.Series) -> bool:
    try:
        fecha_na = pd.isna(row.get("Fecha"))
        desc_blank = str(row.get("Descripción", "")).strip() == ""
        deb = float(row.get("Débito", 0.0) or 0.0)
        cred = float(row.get("Crédito", 0.0) or 0.0)
        saldo = float(row.get("Saldo", 0.0) or 0.0)
        return fecha_na and desc_blank and deb == 0.0 and cred == 0.0 and saldo == 0.0
    except Exception:
        return False


def _severity_rank(sev: str) -> int:
    order = {"CRITICA": 0, "MEDIA": 1, "INFORMATIVA": 2}
    return order.get(str(sev).upper(), 9)


def _severity_badge_html(sev: str) -> str:
    sev_u = str(sev).upper()
    if sev_u == "CRITICA":
        return '<span class="alert-badge alert-critical">✖ Crítica</span>'
    if sev_u == "MEDIA":
        return '<span class="alert-badge alert-medium">⚠ Media</span>'
    return '<span class="alert-badge alert-info">ℹ Inferior</span>'


def _validate_result_df(
    df: pd.DataFrame,
    bank: str,
    source_name: str = "",
    expected_pages: Optional[int] = None,
) -> tuple[dict, pd.DataFrame]:
    if df is None or df.empty:
        summary = {
            "estado": "AMARILLO",
            "criticas": 1,
            "medias": 0,
            "informativas": 0,
            "movimientos": 0,
        }
        alerts = pd.DataFrame([{
            "Severidad": "CRITICA",
            "Archivo": source_name or "-",
            "Fila": "-",
            "Tipo": "Sin movimientos",
            "Detalle": "No se detectaron movimientos válidos en la salida.",
        }])
        return summary, alerts

    work = df.copy().reset_index(drop=True)

    mask_valid = ~work.apply(_is_blank_visual_row, axis=1)
    work = work[mask_valid].reset_index(drop=True)

    alerts: List[dict] = []

    if work.empty:
        summary = {
            "estado": "AMARILLO",
            "criticas": 1,
            "medias": 0,
            "informativas": 0,
            "movimientos": 0,
        }
        alerts = pd.DataFrame([{
            "Severidad": "CRITICA",
            "Archivo": source_name or "-",
            "Fila": "-",
            "Tipo": "Sin movimientos",
            "Detalle": "La salida quedó vacía luego de remover filas visuales en blanco.",
        }])
        return summary, alerts

    work["Fecha"] = pd.to_datetime(work["Fecha"], errors="coerce")
    for c in ["Débito", "Crédito", "Saldo"]:
        work[c] = work[c].apply(_coerce_number)

    # 1) fecha + descripción pero sin importe
    mask_no_amount = (
        work["Fecha"].notna()
        & work["Descripción"].astype(str).str.strip().ne("")
        & (work["Débito"].abs() == 0)
        & (work["Crédito"].abs() == 0)
    )
    for idx in work.index[mask_no_amount]:
        alerts.append({
            "Severidad": "CRITICA",
            "Archivo": source_name or "-",
            "Fila": int(idx) + 2,
            "Tipo": "Movimiento sin importe",
            "Detalle": "La fila tiene fecha y descripción, pero Débito y Crédito están en cero.",
        })

    # 2) débito y crédito simultáneos
    mask_both = (work["Débito"].abs() > 0) & (work["Crédito"].abs() > 0)
    for idx in work.index[mask_both]:
        alerts.append({
            "Severidad": "CRITICA",
            "Archivo": source_name or "-",
            "Fila": int(idx) + 2,
            "Tipo": "Débito y crédito simultáneos",
            "Detalle": "La fila tiene importe en ambas columnas, lo cual es inusual para un extracto.",
        })

    # 3) descripción vacía
    mask_desc_blank = (
        work["Fecha"].notna()
        & ((work["Débito"].abs() > 0) | (work["Crédito"].abs() > 0))
        & work["Descripción"].astype(str).str.strip().eq("")
    )
    for idx in work.index[mask_desc_blank]:
        alerts.append({
            "Severidad": "MEDIA",
            "Archivo": source_name or "-",
            "Fila": int(idx) + 2,
            "Tipo": "Descripción vacía",
            "Detalle": "Movimiento con importe pero sin texto descriptivo.",
        })

    # 4) saldo corrido inconsistente
    tol = 0.01
    for i in range(1, len(work)):
        prev_saldo = float(work.loc[i - 1, "Saldo"])
        deb = float(work.loc[i, "Débito"])
        cred = float(work.loc[i, "Crédito"])
        saldo_real = float(work.loc[i, "Saldo"])
        esperado = round(prev_saldo - deb + cred, 2)
        diff = round(saldo_real - esperado, 2)

        if abs(diff) > tol:
            alerts.append({
                "Severidad": "CRITICA",
                "Archivo": source_name or "-",
                "Fila": int(i) + 2,
                "Tipo": "Saldo inconsistente",
                "Detalle": f"Saldo esperado {esperado:,.2f} y se obtuvo {saldo_real:,.2f}.",
            })

    # 5) importe sospechoso igual al saldo
    for i in range(len(work)):
        deb = abs(float(work.loc[i, "Débito"]))
        cred = abs(float(work.loc[i, "Crédito"]))
        saldo = abs(float(work.loc[i, "Saldo"]))
        imp = max(deb, cred)

        if imp > 0 and saldo > 0 and abs(imp - saldo) <= 0.01:
            alerts.append({
                "Severidad": "MEDIA",
                "Archivo": source_name or "-",
                "Fila": int(i) + 2,
                "Tipo": "Importe sospechoso",
                "Detalle": "El importe coincide exactamente con el saldo; conviene revisar la fila.",
            })

    # 6) pocos movimientos para muchas páginas
    if expected_pages is not None and expected_pages >= 3 and len(work) < expected_pages * 3:
        alerts.append({
            "Severidad": "INFORMATIVA",
            "Archivo": source_name or "-",
            "Fila": "-",
            "Tipo": "Pocos movimientos detectados",
            "Detalle": f"Se detectaron {len(work)} movimientos para {expected_pages} página(s).",
        })

    alerts_df = pd.DataFrame(alerts, columns=["Severidad", "Archivo", "Fila", "Tipo", "Detalle"])

    crit = 0 if alerts_df.empty else int((alerts_df["Severidad"] == "CRITICA").sum())
    med = 0 if alerts_df.empty else int((alerts_df["Severidad"] == "MEDIA").sum())
    inf = 0 if alerts_df.empty else int((alerts_df["Severidad"] == "INFORMATIVA").sum())

    if crit > 0:
        estado = "AMARILLO"
    elif med > 0 or inf > 0:
        estado = "AMARILLO"
    else:
        estado = "VERDE"

    summary = {
        "estado": estado,
        "criticas": crit,
        "medias": med,
        "informativas": inf,
        "movimientos": int(len(work)),
    }

    if not alerts_df.empty:
        alerts_df = alerts_df.sort_values(
            by=["Severidad", "Fila"],
            key=lambda s: s.map(_severity_rank) if s.name == "Severidad" else s,
        ).reset_index(drop=True)

    return summary, alerts_df


def _render_validation_panel(summary: dict, alerts_df: pd.DataFrame):
    estado = summary.get("estado", "VERDE")
    crit = summary.get("criticas", 0)
    med = summary.get("medias", 0)
    inf = summary.get("informativas", 0)

    if estado == "VERDE":
        estado_html = '<span class="status-green">🟢 Verde — Sin observaciones importantes</span>'
    else:
        estado_html = '<span class="status-yellow">🟡 Amarillo — Revisar</span>'

    st.markdown(
        f"""
        <div class="validation-card">
            <div class="validation-title">⚠️ Control automático del resultado</div>
            <div class="validation-summary">
                <div class="validation-status">Estado general: {estado_html}</div>
                <ul class="validation-bullets">
                    <li>{crit} alertas críticas</li>
                    <li>{med} alertas medias</li>
                    <li>{inf} advertencia informativa</li>
                </ul>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if alerts_df is not None and not alerts_df.empty:
        alerts_view = alerts_df.copy()

        def _sev_label(sev: str) -> str:
            sev_u = str(sev).upper()
            if sev_u == "CRITICA":
                return "✖ Crítica"
            if sev_u == "MEDIA":
                return "⚠ Media"
            return "ℹ Inferior"

        alerts_view["Severidad"] = alerts_view["Severidad"].apply(_sev_label)

        st.dataframe(
            alerts_view,
            use_container_width=True,
            height=min(360, 56 + len(alerts_view) * 35),
        )

def _format_date_display(dt) -> str:
    if pd.isna(dt):
        return "-"
    try:
        return pd.to_datetime(dt).strftime("%d/%m/%Y")
    except Exception:
        return "-"


def _format_money_display(x: float) -> str:
    try:
        v = float(x or 0.0)
    except Exception:
        v = 0.0
    s = f"{v:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return s


def _build_summary_data(
    df: pd.DataFrame,
    bank: str,
    files_count: int,
    summary_validation: Optional[dict] = None,
) -> dict:
    if df is None or df.empty:
        return {
            "Banco": bank,
            "Archivos": files_count,
            "Movimientos": 0,
            "Fecha inicial": "-",
            "Fecha final": "-",
            "Total Débitos": 0.0,
            "Total Créditos": 0.0,
            "Saldo final": 0.0,
            "Estado": "Revisar",
        }

    work = df.copy()
    work["Fecha"] = pd.to_datetime(work["Fecha"], errors="coerce")
    for c in ["Débito", "Crédito", "Saldo"]:
        work[c] = work[c].apply(_coerce_number)

    work = work[~work.apply(_is_blank_visual_row, axis=1)].reset_index(drop=True)

    fecha_min = work["Fecha"].min() if not work.empty else pd.NaT
    fecha_max = work["Fecha"].max() if not work.empty else pd.NaT

    total_deb = float(work["Débito"].sum()) if not work.empty else 0.0
    total_cred = float(work["Crédito"].sum()) if not work.empty else 0.0
    saldo_final = float(work.iloc[-1]["Saldo"]) if not work.empty else 0.0

    estado = "OK"
    if summary_validation and summary_validation.get("estado") != "VERDE":
        estado = "Revisar"

    return {
        "Banco": bank,
        "Archivos": files_count,
        "Movimientos": int(len(work)),
        "Fecha inicial": _format_date_display(fecha_min),
        "Fecha final": _format_date_display(fecha_max),
        "Total Débitos": total_deb,
        "Total Créditos": total_cred,
        "Saldo final": saldo_final,
        "Estado": estado,
    }


def _render_summary_panel(summary_data: dict):
    estado = summary_data.get("Estado", "OK")

    estado_html = (
        '<span class="summary-status-ok">🟢 OK</span>'
        if estado == "OK"
        else '<span class="summary-status-review">🟡 Revisar</span>'
    )

    banco = summary_data.get("Banco", "-")
    movs = summary_data.get("Movimientos", 0)
    deb = _format_money_display(summary_data.get("Total Débitos", 0.0))
    cred = _format_money_display(summary_data.get("Total Créditos", 0.0))
    saldo = _format_money_display(summary_data.get("Saldo final", 0.0))

    st.markdown(
        f"""
        <div class="summary-inline">
            <div><strong>{banco}</strong></div>
            <div>Mov: <strong>{movs}</strong></div>
            <div>Déb: $ {deb}</div>
            <div>Créd: $ {cred}</div>
            <div>Saldo: $ {saldo}</div>
            <div>{estado_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _friendly_error_info(error_text: str):
    e = (error_text or "").lower()

    if "openpyxl" in e:
        return {
            "titulo": "Falta una librería necesaria para la clasificación",
            "causa": "La app intentó leer el archivo de clasificación Excel, pero no encontró la dependencia openpyxl.",
            "solucion": "Verificá que openpyxl esté incluido en requirements.txt y que la app haya redeployado correctamente.",
        }

    if "no se pudo detectar automáticamente el banco" in e:
        return {
            "titulo": "No se pudo detectar el banco automáticamente",
            "causa": "La opción AUTO no encontró un patrón claro en el PDF para identificar el banco.",
            "solucion": "Elegí el banco manualmente desde el selector y volvé a procesar el archivo.",
        }

    if "tiempo de procesamiento excedido" in e:
        return {
            "titulo": "El archivo tardó demasiado en procesarse",
            "causa": "El PDF puede ser muy grande, tener muchas páginas o requerir un análisis más pesado de lo normal.",
            "solucion": "Probá procesar menos archivos a la vez, activar el Modo LARGO o dividir la tanda en partes más chicas.",
        }

    if "multiple banks" in e or "múltiples bancos" in e:
        return {
            "titulo": "Se detectaron varios bancos en una misma carga",
            "causa": "La app encontró PDFs de distintos bancos dentro del mismo procesamiento.",
            "solucion": "Separá los archivos por banco y procesá cada grupo por separado.",
        }

    if "maximo" in e or "máximo" in e or "supera" in e:
        return {
            "titulo": "La carga supera los límites permitidos",
            "causa": "La cantidad de archivos o el peso total de la tanda es mayor al permitido por la app.",
            "solucion": "Subí menos archivos por tanda o dividí la carga en varios procesos.",
        }

    if "permission" in e or "permiso" in e:
        return {
            "titulo": "Problema de permisos o acceso",
            "causa": "La app no pudo acceder correctamente a un archivo o recurso requerido.",
            "solucion": "Volvé a intentar el proceso y, si persiste, revisá que los archivos se hayan cargado bien.",
        }

    if "excel" in e and "clasif" in e:
        return {
            "titulo": "Problema al leer el archivo de clasificación",
            "causa": "La app no pudo abrir o interpretar el Excel de clasificación de movimientos.",
            "solucion": "Revisá que el archivo CLASIFICACION EXTRACTOS.xlsx esté bien subido y con el formato esperado.",
        }

    return {
        "titulo": "Ocurrió un error al procesar el archivo",
        "causa": "La app encontró un problema durante el análisis del PDF o la generación del Excel.",
        "solucion": "Revisá el banco seleccionado, probá con menos archivos o consultá la pestaña Registro para ver el detalle técnico.",
    }

def _show_main_error(errors: List[str]):
    if not errors:
        return

    first_error = errors[0]
    info = _friendly_error_info(first_error)

    st.error(
        f"""**{info['titulo']}**

**Posible causa:** {info['causa']}

**Solución rápida:** {info['solucion']}

**Detalle detectado:** `{first_error}`

Para ver más información técnica, revisá la pestaña **Registro**."""
    )

if do_convert:
    if not files:
        st.warning("Subí al menos un PDF.")
    else:
        progress_box = st.empty()

        inputs: List[Tuple[str, bytes, str]] = []
        for f in files:
            data = f.read()
            inputs.append((f.name, data, _bytes_hash(data)))
        inputs.sort(key=lambda x: x[0])

        total_inputs = len(inputs)
        _render_progress(
            progress_box,
            current=0,
            total=total_inputs,
            label="Preparando conversión",
            sublabel=f"Se detectaron {total_inputs} archivo(s) para procesar.",
        )

        if long_mode:
            timeouts = {
                "BANCOR": 600,
                "GALICIA": 480,
                "ICBC": 480,
                "PATAGONIA": 420,
                "MACRO": 600,
                "MACRO 2": 600,
                "NACION": 480,
                "NACION 2": 480,
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
                "NACION 2": 240,
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
                
                if isinstance(raw, pd.DataFrame):
                    fin = raw.copy()
                else:
                    try:
                        fin = pd.DataFrame(raw)
                    except Exception:
                        fin = pd.DataFrame(columns=EXPECTED_COLS)

                fin = _normalize_df(fin)
                fin = _ensure_columns_for_export(fin)

                if detected_bank in FIXES:
                    fin = FIXES[detected_bank](fin)

                fin = _ensure_columns_for_export(fin)

                if do_classification:
                    fin = _apply_classification(fin, detected_bank)
                    fin = _ensure_columns_for_export(fin)

                # mantener comportamiento actual de orden
                if detected_bank not in {"SUPERVIELLE 2", "NACION 2"}:
                    fin = _sort_rows_by_fecha(fin)

            pages = _count_pdf_pages(data)
            _log(f"✓ {name}: {detected_bank} | {len(fin)} filas en {time.time() - t0:.1f}s")
            return fin, detected_bank, pages
            
        items = []
        errors = []

        if max_workers == 1:
            for idx, (name, data, _h) in enumerate(inputs, start=1):
                try:
                    _render_progress(
                        progress_box,
                        current=idx - 1,
                        total=total_inputs,
                        label="Procesando archivos",
                        sublabel=f"Procesando {idx}/{total_inputs}: {name}",
                    )

                    if bank == "AUTO":
                        provisional_bank = detect_bank(data)
                        if not provisional_bank:
                            raise ValueError(f"No se pudo detectar automáticamente el banco de {name}")
                        timeout_sec = timeouts.get(provisional_bank, 300 if not long_mode else 600)
                    else:
                        timeout_sec = timeouts.get(bank, 300 if not long_mode else 600)

                    fin, detected_bank, pages = process_one(name, data, timeout_sec)
                    mind = _df_min_date(fin)
                    items.append((name, fin, mind, detected_bank, pages))

                    _render_progress(
                        progress_box,
                        current=idx,
                        total=total_inputs,
                        label="Procesando archivos",
                        sublabel=f"Completado {idx}/{total_inputs}: {name}",
                    )

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
                done_count = 0

                for fut in as_completed(fut_map):
                    name, data = fut_map[fut]
                    try:
                        tmp[name] = fut.result()
                        done_count += 1
                        _render_progress(
                            progress_box,
                            current=done_count,
                            total=total_inputs,
                            label="Procesando archivos",
                            sublabel=f"Completado {done_count}/{total_inputs}: {name}",
                        )
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
                        fin, detected_bank, pages = tmp[name]
                        mind = _df_min_date(fin)
                        items.append((name, fin, mind, detected_bank, pages))

        if errors:
            _show_main_error(errors)

        if errors and atomic_mode:
            progress_box.empty()
            tab_log.error("Se canceló la generación del Excel (modo atómico activo).")
            for e in errors:
                tab_log.write("• " + e)

            _add_history_entry(
                bank_selected=bank,
                effective_bank=bank,
                files_names=[name for name, _, _ in inputs],
                status="ERROR",
                rows_count=0,
                output_name="-",
            )

        elif items:
            _render_progress(
                progress_box,
                current=total_inputs,
                total=total_inputs,
                label="Finalizando",
                sublabel="Armando vista previa y archivo Excel...",
            )

            sortable = []
            for name, fin, mind, detected_bank, pages in items:
                if pd.isna(mind):
                    y, m = _infer_period_from_filename(name)
                    if y and m:
                        mind = pd.Timestamp(year=y, month=m, day=1)
                sortable.append((name, fin, mind, detected_bank, pages))

            def _key(t):
                name, fin, mind, detected_bank, pages = t
                has = 0 if (isinstance(mind, pd.Timestamp) and not pd.isna(mind)) else 1
                return (has, mind if not pd.isna(mind) else pd.Timestamp.max, name.lower())

            sortable.sort(key=_key)

            if bank == "AUTO":
                detected_banks = sorted(set(db for _, _, _, db, _ in sortable))
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

                for name, fin, _mind, _detected_bank, _pages in sortable:
                    if "Cuenta" in fin.columns:
                        for cta, chunk in fin.groupby(fin["Cuenta"].fillna("GENERAL"), sort=False):
                            chunk = chunk.drop(columns=[c for c in ["Cuenta"] if c in chunk.columns])
                            chunk = _ensure_columns_for_export(chunk)
                            account_map.setdefault(str(cta), []).append(chunk[EXPECTED_COLS])
                    else:
                        fin = _ensure_columns_for_export(fin)
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

                excel_name = f"EXTRACTOS_{effective_bank}.xlsx"

                first_name, _first_fin, _first_mind, _first_detected_bank, first_pages = sortable[0]
                summary, alerts_df = _validate_result_df(
                    result_preview,
                    bank=effective_bank,
                    source_name=first_name,
                    expected_pages=first_pages,
                )

                summary_data = _build_summary_data(
                    result_preview,
                    bank=effective_bank,
                    files_count=len(sortable),
                    summary_validation=summary,
                )

                with tab_prev:
                    _render_summary_panel(summary_data)

                    st.download_button(
                        "⬇️ Descargar Excel (NUMÉRICO, múltiples hojas por cuenta)",
                        data=buf.getvalue(),
                        file_name=excel_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )

                    _render_validation_panel(summary, alerts_df)

                    st.subheader(f"Vista previa (hoja: {first_sheet})")
                    st.dataframe(result_preview, use_container_width=True, height=480)

                total_rows = sum(len(_append_blocks(account_map[cta])) for cta in account_map.keys())

                _add_history_entry(
                    bank_selected=bank,
                    effective_bank=effective_bank,
                    files_names=[name for name, _, _, _, _ in sortable],
                    status="OK",
                    rows_count=total_rows,
                    output_name=excel_name,
                )

            else:
                chunks = []

                for name, fin, _mind, _detected_bank, _pages in sortable:
                    chunks.append(fin[EXPECTED_COLS])

                    if add_blank:
                        chunks.append(
                            pd.DataFrame(
                                [[pd.NaT, "", "", 0.0, 0.0, 0.0]],
                                columns=EXPECTED_COLS,
                            )
                        )

                result = _append_blocks(chunks)

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

                excel_name = f"EXTRACTOS_{effective_bank}.xlsx"

                first_name, _first_fin, _first_mind, _first_detected_bank, first_pages = sortable[0]
                summary, alerts_df = _validate_result_df(
                    result,
                    bank=effective_bank,
                    source_name=first_name,
                    expected_pages=first_pages,
                )

                summary_data = _build_summary_data(
                    result,
                    bank=effective_bank,
                    files_count=len(sortable),
                    summary_validation=summary,
                )

                with tab_prev:
                    _render_summary_panel(summary_data)

                    st.download_button(
                        "⬇️ Descargar Excel (NUMÉRICO)",
                        data=buf.getvalue(),
                        file_name=excel_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )

                    _render_validation_panel(summary, alerts_df)

                    st.subheader("Vista previa")
                    st.dataframe(result, use_container_width=True, height=480)

                _add_history_entry(
                    bank_selected=bank,
                    effective_bank=effective_bank,
                    files_names=[name for name, _, _, _, _ in sortable],
                    status="OK",
                    rows_count=len(result),
                    output_name=excel_name,
                )

            else:
                chunks = []

                for name, fin, _mind, _detected_bank, _pages in sortable:
                    chunks.append(fin[EXPECTED_COLS])

                    if add_blank:
                        chunks.append(
                            pd.DataFrame(
                                [[pd.NaT, "", "", 0.0, 0.0, 0.0]],
                                columns=EXPECTED_COLS,
                            )
                        )

                result = _append_blocks(chunks)


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

                excel_name = f"EXTRACTOS_{effective_bank}.xlsx"

                first_name, _first_fin, _first_mind, _first_detected_bank, first_pages = sortable[0]
                summary, alerts_df = _validate_result_df(
                    result,
                    bank=effective_bank,
                    source_name=first_name,
                    expected_pages=first_pages,
                )

				summary_data = _build_summary_data(
    				result,
    				bank=effective_bank,
    				files_count=len(sortable),
    				summary_validation=summary,
				)
                
                with tab_prev:
		    		_render_summary_panel(summary_data)

                    st.download_button(
                        "⬇️ Descargar Excel (NUMÉRICO)",
                        data=buf.getvalue(),
                        file_name=excel_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )
                    _render_validation_panel(summary, alerts_df)
                    
                    st.subheader("Vista previa")
                    st.dataframe(result, use_container_width=True, height=480)

                _add_history_entry(
                    bank_selected=bank,
                    effective_bank=effective_bank,
                    files_names=[name for name, _, _, _, _ in sortable],
                    status="OK",
                    rows_count=len(result),
                    output_name=excel_name,
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

        with tab_hist:
            st.subheader("Historial de esta sesión")

            if st.session_state.history:
                df_hist = pd.DataFrame(st.session_state.history)
                st.dataframe(df_hist, use_container_width=True, height=320)
            else:
                st.info("Todavía no hay procesamientos registrados.")

with tab_hist:
    if not do_convert:
        st.subheader("Historial de esta sesión")

        if st.session_state.history:
            df_hist = pd.DataFrame(st.session_state.history)
            st.dataframe(df_hist, use_container_width=True, height=320)
        else:
            st.info("Todavía no hay procesamientos registrados.")

with tab_log:
    if not do_convert and logs:
        st.subheader("Registro")
        for line in logs:
            st.write(line)

st.markdown("""
<div class="author">
  <div>
    <div class="tiny">autor:</div>
    <div class="name">Tadeo Balfagon</div>
  </div>
</div>
""", unsafe_allow_html=True)
