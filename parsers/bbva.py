# parsers/bbva.py
# Wrapper para usar SIN CAMBIAR NADA tu process_bbva_v2.py
# - Carga process_bbva_v2 desde raíz o ./parsers
# - Acepta BYTES y llama a parse_bbva_pdf() o process_bbva()
# - Convierte el dict{Cuenta->DF} a un único DataFrame con columna 'Cuenta'
# - Compatible con Windows (evita Permission Denied en archivos temporales)

from __future__ import annotations
import importlib
import importlib.util
import os
import sys
import tempfile
from typing import Dict, List

import pandas as pd


# ---------------- Carga del módulo V2 (sin modificarlo) ----------------
def _try_import_by_name(modname: str):
    try:
        return importlib.import_module(modname)
    except Exception:
        return None


def _try_import_by_path(path: str, modname: str):
    if not os.path.isfile(path):
        return None
    spec = importlib.util.spec_from_file_location(modname, path)
    if spec is None or spec.loader is None:
        return None
    m = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(m)  # type: ignore[attr-defined]
        sys.modules[modname] = m
        return m
    except Exception:
        return None


def _load_v2_module():
    # A) import por nombre
    for name in ("process_bbva_v2", "parsers.process_bbva_v2"):
        m = _try_import_by_name(name)
        if m:
            return m

    # B) import por ruta
    here = os.path.dirname(os.path.abspath(__file__))  # .../parsers
    root = os.path.dirname(here)                       # raíz del proyecto

    candidates = [
        os.path.join(root, "process_bbva_v2.py"),
        os.path.join(here, "process_bbva_v2.py"),
    ]
    for p in candidates:
        m = _try_import_by_path(p, os.path.splitext(os.path.basename(p))[0])
        if m:
            return m

    raise ModuleNotFoundError(
        "No se encontró 'process_bbva_v2.py'. Colocalo en la raíz del proyecto o dentro de 'parsers/'."
    )


_mod = _load_v2_module()

_HAS_PARSE_V2 = hasattr(_mod, "parse_bbva_pdf") and callable(getattr(_mod, "parse_bbva_pdf"))
_HAS_PROCESS_V2 = hasattr(_mod, "process_bbva") and callable(getattr(_mod, "process_bbva"))

if not (_HAS_PARSE_V2 or _HAS_PROCESS_V2):
    raise RuntimeError(
        "Tu 'process_bbva_v2.py' no expone ni 'parse_bbva_pdf' ni 'process_bbva'. "
        "Debe existir al menos una de esas funciones (como en tu app individual)."
    )


# ---------------- Helpers ----------------
def _dict_to_df(sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Convierte dict{cuenta -> DataFrame} a un único DataFrame con columna 'Cuenta'.
    No normaliza ni modifica valores.
    """
    frames: List[pd.DataFrame] = []
    for cuenta, df in (sheets or {}).items():
        if df is None or df.empty:
            continue
        tmp = df.copy()
        if "Cuenta" not in tmp.columns:
            tmp["Cuenta"] = str(cuenta)
        frames.append(tmp)

    if not frames:
        return pd.DataFrame(columns=["Fecha", "Descripción", "Débito", "Crédito", "Saldo", "Cuenta"])

    out = pd.concat(frames, ignore_index=True)
    cols = [c for c in ["Fecha", "Descripción", "Débito", "Crédito", "Saldo", "Cuenta"] if c in out.columns]
    return out[cols] if cols else out


# ---------------- API usada por app_unica.py ----------------
def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    """
    Entrada para la app unificada:
      - Recibe BYTES del PDF
      - Escribe un temporal (cerrándolo antes de leer) para evitar errores en Windows
      - Invoca tu V2 (parse_bbva_pdf o process_bbva)
      - Devuelve UN DataFrame con 'Cuenta'
    """
    # Crear temporal compatible con Windows
    tmp = tempfile.NamedTemporaryFile(prefix="bbva_", suffix=".pdf", delete=False)
    try:
        tmp.write(file_bytes)
        tmp.flush()
        tmp_path = tmp.name
    finally:
        # CERRAR antes de que lo use pdfplumber (fix Permission Denied en Windows)
        try:
            tmp.close()
        except Exception:
            pass

    try:
        if _HAS_PARSE_V2:
            sheets = _mod.parse_bbva_pdf(tmp_path)  # tu V2 típico
        else:
            sheets = _mod.process_bbva([tmp_path])  # alternativa V2

    finally:
        # Borrar el temporal si es posible
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    return _dict_to_df(sheets)
