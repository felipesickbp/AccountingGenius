"""
raiffeisen_transform.py – v1.3
——————————————
* forces every account value to string → no “6530.0”, MWST test works
* builds Soll/Haben & MWST columns with safe strings
"""

from __future__ import annotations
import io
from typing import Union, BinaryIO

import pandas as pd
from bookkeeping_app import KontierungEngine

_MWST_ACCOUNTS = {"6210", "6260", "6510", "6530", "6640"}


def _load_xl(file: Union[str, bytes, BinaryIO]) -> pd.DataFrame:
    if isinstance(file, (str, bytes, bytearray)):
        return pd.read_excel(file, header=None, engine="openpyxl")
    return pd.read_excel(file, header=None, engine="openpyxl")


def process_excel(
    uploaded_file: Union[str, bytes, io.BufferedReader],
    engine: KontierungEngine,
    start_no: int = 1,
) -> pd.DataFrame:
    # ── 0 read raw
    df_raw = _load_xl(uploaded_file)

    # ── 1 drop IBAN
    df = df_raw.drop(columns=0)
    df.columns = range(df.shape[1])

    # ── 2 date → dd.mm.yyyy
    df[0] = pd.to_datetime(df[0], errors="coerce").dt.strftime("%d.%m.%Y")

    # ── 3 merge continuation lines
    to_del = []
    for i in range(1, len(df)):
        if pd.isna(df.iat[i, 0]) and df.iloc[i, 2:].isna().all():
            if not pd.isna(df.iat[i, 1]):
                df.iat[i - 1, 1] = f"{df.iat[i - 1, 1]} {df.iat[i, 1]}".strip()
            to_del.append(i)
    if to_del:
        df = df.drop(index=to_del).reset_index(drop=True)
    df[0].fillna(method="ffill", inplace=True)

    # ── 4 numeric amount, Soll/Haben
    df[2] = (
        df[2]
        .astype(str)
        .str.replace("CHF", "", regex=False)
        .str.replace("'", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(" ", "", regex=False)
    )
    df[2] = pd.to_numeric(df[2], errors="coerce").abs()

    # account as **string**
    df["account"] = (
        df[1].astype(str).apply(engine.classify).apply(lambda x: str(x) if x else "")
    )

    df["soll"] = df.apply(
        lambda r: "1020" if r[2] > 0 else r["account"], axis=1
    )
    df["haben"] = df.apply(
        lambda r: "1020" if r[2] < 0 else r["account"], axis=1
    )

    # ── 5 MWST
    df["MWST Code"] = df["account"].apply(
        lambda a: "VB81" if a in _MWST_ACCOUNTS else ""
    )
    df["MWST Konto"] = df["account"].apply(
        lambda a: a if a in _MWST_ACCOUNTS else ""
    )

    # ── 6 assemble
    out = pd.DataFrame(
        {
            "Belegnummer": range(int(start_no), int(start_no) + len(df)),
            "date":         df[0],
            "description":  df[1],
            "amount":       df[2],
            "soll":         df["soll"],
            "haben":        df["haben"],
            "needs_review": df["account"].eq(""),
            "MWST Code":    df["MWST Code"],
            "MWST Konto":   df["MWST Konto"],
        }
    )
    return out
