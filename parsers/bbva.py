# parsers/bbva.py
# Wrapper para usar SIN CAMBIAR NADA tu process_bbva_v2.py

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
import tempfile
from typing import Dict, List

import pandas as pd
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
        spec.loader.exec_module(m)
        sys.modules[modname] = m
        return m
    except Exception:
        return None
def _load_v2_module():

    for name in ("process_bbva_v2", "parsers.process_bbva_v2"):
        m = _try_import_by_name(name)
        if m:
            return m

    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)

    candidates = [
        os.path.join(root, "process_bbva_v2.py"),
        os.path.join(here, "process_bbva_v2.py"),
    ]

    for p in candidates:
        m = _try_import_by_path(p, os.path.splitext(os.path.basename(p))[0])
        if m:
            return m

    raise ModuleNotFoundError(
        "No se encontró 'process_bbva_v2.py'."
    )
def parse_pdf(file_bytes: bytes) -> pd.DataFrame:

    tmp = tempfile.NamedTemporaryFile(
        prefix="bbva_",
        suffix=".pdf",
        delete=False
    )

    try:
        tmp.write(file_bytes)
        tmp.flush()
        tmp_path = tmp.name

    finally:
        tmp.close()

    try:

        if hasattr(_mod, "parse_bbva_pdf"):
            sheets = _mod.parse_bbva_pdf(tmp_path)

        else:
            sheets = _mod.process_bbva([tmp_path])

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    return _dict_to_df(sheets)
