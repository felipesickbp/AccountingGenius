"""
raiffeisen_transform.py – v1.2
——————————————
Parses Raiffeisen XLS/XLSX statements and delivers a 10-column
ledger ready for export:

    Belegnummer | date | description | amount | soll | haben
    | needs_review | MWST Code | MWST Konto

Additions in this version
=========================
* Prepends a running Belegnummer (counter) – default start = 1
* Adds MWST Code + MWST Konto:
      – if booking account ∈ {6210, 6260, 6510, 6530, 6640}
        → MWST Code  = "VB81"
          MWST Konto = same account number
"""

from __future__ import annotations
import io
from typing import Union, BinaryIO

import pandas as pd

from bookkeeping_app import KontierungEngine


# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #
# Was:
# MWST_ACCOUNTS = {"6210", "6260", "6510", "6530", "6640"}

MWST_ACCOUNTS = {
    "1500", "1510", "1520", "1530",        # Maschinen, Mobiliar, IT, Fahrzeuge
    "5008",                                # übriger Personalaufwand
    "5810", "5820", "5821", "5880",        # Weiterbildung / Spesen / Anlässe
    "6040",                                # Reinigung
    "6100", "6101",                        # URE (Mobiliar/Informatik)
    "6200", "6210", "6260",                # Fahrzeugaufwand
    "6400",                                # Energie & Entsorgung
    "6500", "6510", "6512", "6513", "6530", "6559", "6570",  # Verwaltung & IT
    "6600",                                # Werbung
    "6640", "6641",                        # Reisespesen / Kundenbetreuung
    "6740",                                # Vorsteuerkorrektur
}

# exactly the same literal for _MWST_ACCOUNTS in raiffeisen_transform.py



def _load_xl(file: Union[str, bytes, BinaryIO]) -> pd.DataFrame:
    """Accept path-like, bytes, or file-like."""
    if isinstance(file, (str, bytes, bytearray)):
        return pd.read_excel(file, header=None, engine="openpyxl")
    # Streamlit passes an UploadedFile → file-like
    return pd.read_excel(file, header=None, engine="openpyxl")


# --------------------------------------------------------------------------- #
#  Public API                                                                  #
# --------------------------------------------------------------------------- #
def process_excel(
    uploaded_file: Union[str, bytes, io.BufferedReader],
    engine: KontierungEngine,
    start_no: int = 1,                     # ← where the counter begins
) -> pd.DataFrame:
    """
    Parse a Raiffeisen statement and return a tidy ledger DataFrame.

    Parameters
    ----------
    uploaded_file : str | bytes | BinaryIO
        Incoming XLS/XLSX file (path, bytes or IO stream).
    engine : KontierungEngine
        Keyword→account classifier inherited from bookkeeping_app.py.
    start_no : int, default 1
        First voucher number (Belegnummer).

    Returns
    -------
    pandas.DataFrame
        Columns (in order):
        [Belegnummer, date, description, amount, soll, haben,
         needs_review, MWST Code, MWST Konto]
    """
    # ------------------------------------------------------------------ 0  read raw
    df_raw = _load_xl(uploaded_file)

    # ------------------------------------------------------------------ 1  drop IBAN
    df = df_raw.drop(columns=0)
    df.columns = range(df.shape[1])        # rename to 0,1,2,...

    # ------------------------------------------------------------------ 2  date format
    df[0] = pd.to_datetime(df[0], errors="coerce").dt.strftime("%d.%m.%Y")

    # ------------------------------------------------------------------ 3  merge continuation lines
    to_delete = []
    for i in range(1, len(df)):
        if pd.isna(df.iat[i, 0]) and df.iloc[i, 2:].isna().all():
            if not pd.isna(df.iat[i, 1]):
                prev_txt = str(df.iat[i - 1, 1]) if not pd.isna(df.iat[i - 1, 1]) else ""
                df.iat[i - 1, 1] = f"{prev_txt} {df.iat[i, 1]}".strip()
            to_delete.append(i)

    if to_delete:
        df = df.drop(index=to_delete).reset_index(drop=True)

    df[0].fillna(method="ffill", inplace=True)   # in case top rows were NaN

    # ------------------------------------------------------------------ 4  numeric amount, soll/haben
    df[2] = (
        df[2]
        .astype(str)
        .str.replace("CHF", "", regex=False)
        .str.replace("'", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(" ", "", regex=False)
    )
    df[2] = pd.to_numeric(df[2], errors="coerce")

    df["account"] = df[1].astype(str).apply(engine.classify)
    df["soll"]   = df.apply(lambda r: "1020" if r[2] > 0 else (r["account"] or ""), axis=1)
    df["haben"]  = df.apply(lambda r: "1020" if r[2] < 0 else (r["account"] or ""), axis=1)
    df[2] = df[2].abs()

    # ------------------------------------------------------------------ 5  MWST logic
    def _mwst_code(acct: str) -> str:
        return "VB81" if acct in _MWST_ACCOUNTS else ""

    df["MWST Code"]  = df["account"].apply(_mwst_code)
    df["MWST Konto"] = df["account"].apply(
        lambda a: a if a in _MWST_ACCOUNTS else ""
    )

    # ------------------------------------------------------------------ 6  assemble output
    out = pd.DataFrame(
        {
            "Belegnummer": range(int(start_no), int(start_no) + len(df)),
            "date":         df[0],
            "description":  df[1],
            "amount":       df[2],
            "soll":         df["soll"],
            "haben":        df["haben"],
            "needs_review": df["account"].isna(),
            "MWST Code":    df["MWST Code"],
            "MWST Konto":   df["MWST Konto"],
        }
    )

    return out
