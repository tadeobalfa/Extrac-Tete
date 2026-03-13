from __future__ import annotations
import importlib.util
import os
import sys
import tempfile
from typing import Dict, List

import pandas as pd


def _load_v2_module():
    """
    Carga de forma EXPLÍCITA y ÚNICA el process_bbva_v2.py
    ubicado en la raíz del proyecto.
    """
    here = os.path.dirname(os.path.abspath(__file__))   # .../parsers
    root = os.path.dirname(here)                        # raíz proyecto
    target = os.path.join(root, "process_bbva_v2.py")

    if not os.path.isfile(target):
        raise ModuleNotFoundError(
            f"No se encontró process_bbva_v2.py en la raíz del proyecto: {target}"
        )

    modname = "_bbva_v2_locked"
    spec = importlib.util.spec_from_file_location(modname, target)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar spec de {target}")

    m = importlib.util.module_from_spec(spec)
    sys.modules[modname] = m
    spec.loader.exec_module(m)  # type: ignore[attr-defined]

    print(f"DEBUG BBVA usando parser: {target}")
    return m


_mod = _load_v2_module()

_HAS_PARSE_V2 = hasattr(_mod, "parse_bbva_pdf") and callable(getattr(_mod, "parse_bbva_pdf"))
_HAS_PROCESS_V2 = hasattr(_mod, "process_bbva") and callable(getattr(_mod, "process_bbva"))

if not (_HAS_PARSE_V2 or _HAS_PROCESS_V2):
    raise RuntimeError(
        "process_bbva_v2.py no expone ni parse_bbva_pdf ni process_bbva"
    )


def _dict_to_df(sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
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


def parse_pdf(file_bytes: bytes) -> pd.DataFrame:
    tmp = tempfile.NamedTemporaryFile(prefix="bbva_", suffix=".pdf", delete=False)
    try:
        tmp.write(file_bytes)
        tmp.flush()
        tmp_path = tmp.name
    finally:
        try:
            tmp.close()
        except Exception:
            pass

    try:
        if _HAS_PARSE_V2:
            sheets = _mod.parse_bbva_pdf(tmp_path)
        else:
            sheets = _mod.process_bbva([tmp_path])
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    out = _dict_to_df(sheets)

    try:
        print("DEBUG BBVA salida preview:")
        print(out.head(15).to_string())
    except Exception:
        pass

    return out
