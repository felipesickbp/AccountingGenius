"""
raiffeisen_transform.py
—————————
Parses Raiffeisen XLS/XLSX statements that look like:

    A = IBAN (drop)      D = amount
    B = date/time        E =  (blank)
    C = description      F =  (blank)

Continuation rows:
    – B empty, D-F empty  → the text in C belongs to the previous row.

Logic recap
===========  (mirrors your earlier prototype)
* drop IBAN column
* date → dd.mm.yyyy
* glue continuation lines upward so every row has a date
* amount → absolute;     + credits in **soll**  /  – debits in **haben**
* put "1020" into the opposite side
"""

from __future__ import annotations
import io
from typing import Union, BinaryIO

import pandas as pd

# reuse the same engine class from bookkeeping_app
from bookkeeping_app import KontierungEngine


def _load_xl(file: Union[str, bytes, BinaryIO]) -> pd.DataFrame:
    """Accept path-like, bytes, or file-like."""
    if isinstance(file, (str, bytes, bytearray)):
        return pd.read_excel(file, header=None)
    # Streamlit gives us an UploadedFile, which is file-like:
    return pd.read_excel(file, header=None)


def process_excel(
    uploaded_file: Union[str, bytes, io.BufferedReader],
    engine: KontierungEngine,
) -> pd.DataFrame:
    df_raw = _load_xl(uploaded_file)

    # ------------------------------------------------------------------ 1 drop IBAN
    df = df_raw.drop(columns=0)
    df.columns = range(df.shape[1])          # rename to 0,1,2,...

    # ------------------------------------------------------------------ 2 date → short Swiss format
    df[0] = pd.to_datetime(df[0], errors="coerce").dt.strftime("%d.%m.%Y")

    # ------------------------------------------------------------------ 3 merge continuation rows
    to_delete = []
    for i in range(1, len(df)):
        # A continuation row has: no date AND D-F empty/NaN
        if pd.isna(df.iat[i, 0]) and df.iloc[i, 2:].isna().all():
            if not pd.isna(df.iat[i, 1]):                                # C not empty
                prev_text = str(df.iat[i - 1, 1]) if not pd.isna(df.iat[i - 1, 1]) else ""
                df.iat[i - 1, 1] = (prev_text + " " + str(df.iat[i, 1])).strip()
            to_delete.append(i)

    if to_delete:
        df = df.drop(index=to_delete).reset_index(drop=True)

    # still might have NaNs at top – forward-fill date
    df[0].fillna(method="ffill", inplace=True)

    # ------------------------------------------------------------------ 4 amount column & 1020 logic
    df[2] = (
        df[2]
        .astype(str)
        .str.replace("CHF", "", regex=False)
        .str.replace("'", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(" ", "", regex=False)
    )
    df[2] = pd.to_numeric(df[2], errors="coerce")

    # ledger columns
    df["account"] = df[1].astype(str).apply(engine.classify)
    df["soll"]  = df.apply(lambda r: "1020" if r[2] > 0 else (r["account"] or ""), axis=1)
    df["haben"] = df.apply(lambda r: "1020" if r[2] < 0 else (r["account"] or ""), axis=1)
    df[2] = df[2].abs()

    # ------------------------------------------------------------------ 5 return tidy ledger
    out = pd.DataFrame(
        {
            "date":        df[0],
            "description": df[1],
            "amount":      df[2],
            "soll":        df["soll"],
            "haben":       df["haben"],
            "needs_review": df["account"].isna(),
        }
    )

    return out
