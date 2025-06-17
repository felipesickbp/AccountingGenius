"""
raiffeisen_transform.py – v1.4
——————————————
Raiffeisen Excel → tidy 10-column ledger

• Cleans the *description* column  
• Corrects **Soll/Haben** logic  
    – money **out** (negative) → *Soll* = expense account, *Haben* = 1020  
    – money **in** (positive) → *Soll* = 1020, *Haben* = revenue / clearing account  
• Adds MWST Code / Konto for VB81 accounts
"""

from __future__ import annotations
import io
import re
from typing import Union, BinaryIO

import pandas as pd
from bookkeeping_app import KontierungEngine

# ──────────────────────────────────────────────────────────────────────────────
# MWST-relevant Konten (VB81 – Inv/BA 8.1 %)
# ──────────────────────────────────────────────────────────────────────────────
_MWST_ACCOUNTS: set[str] = {
    "1500", "1510", "1520", "1530",
    "5008",
    "5810", "5820", "5821", "5880",
    "6040",
    "6100", "6101",
    "6200", "6210", "6260",
    "6400",
    "6500", "6510", "6512", "6513", "6530", "6559", "6570",
    "6600",
    "6640", "6641",
    "6740",
}

# ──────────────────────────────────────────────────────────────────────────────
#  Description-clean helpers
# ──────────────────────────────────────────────────────────────────────────────
_BAD_UMLAUTS = {
    "Ã¼": "ü", "Ã¶": "ö", "Ã¤": "ä", "ÃŸ": "ß",
    "Ãœ": "Ü", "Ã–": "Ö", "Ã„": "Ä",
}

_REMOVE_VISA = re.compile(r"Visa\s+Debit[- ]Nr\.\s*\d{4,6}x{6}\d{4}", re.I)
_REMOVE_CHF  = re.compile(r"\s*CHF\s*[\d'’.,]+")
_REMOVE_KW   = re.compile(
    r"\b(Gutschrift|Online\s+Einkauf|Einkauf|Zahlung|Dauerauftrag)\b",
    re.I,
)


def _fix_umlauts(txt: str) -> str:
    for bad, good in _BAD_UMLAUTS.items():
        txt = txt.replace(bad, good)
    return txt


def clean_description(text: str) -> str:
    """Apply all cleanup rules to the raw Raiffeisen description."""
    text = _fix_umlauts(str(text))
    text = _REMOVE_VISA.sub("", text)
    text = _REMOVE_CHF.sub("", text)
    text = _REMOVE_KW.sub("", text)
    return " ".join(text.split()).strip()


# ──────────────────────────────────────────────────────────────────────────────
#  Excel loader
# ──────────────────────────────────────────────────────────────────────────────
def _load_xl(file: Union[str, bytes, BinaryIO]) -> pd.DataFrame:
    if isinstance(file, (str, bytes, bytearray)):
        return pd.read_excel(file, header=None, engine="openpyxl")
    return pd.read_excel(file, header=None, engine="openpyxl")


# ──────────────────────────────────────────────────────────────────────────────
#  Public API
# ──────────────────────────────────────────────────────────────────────────────
def process_excel(
    uploaded_file: Union[str, bytes, io.BufferedReader],
    engine: KontierungEngine,
    start_no: int = 1,
) -> pd.DataFrame:
    # 0  read raw workbook
    df_raw = _load_xl(uploaded_file)

    # 1  drop IBAN column A
    df = df_raw.drop(columns=0)
    df.columns = range(df.shape[1])

    # 2  dates → Swiss short format
    df[0] = pd.to_datetime(df[0], errors="coerce").dt.strftime("%d.%m.%Y")

    # 3  merge continuation lines (rows with empty date + empty numeric cols)
    to_del: list[int] = []
    for i in range(1, len(df)):
        if pd.isna(df.iat[i, 0]) and df.iloc[i, 2:].isna().all():
            if not pd.isna(df.iat[i, 1]):
                df.iat[i - 1, 1] = f"{df.iat[i - 1, 1]} {df.iat[i, 1]}".strip()
            to_del.append(i)
    if to_del:
        df = df.drop(index=to_del).reset_index(drop=True)
    df[0].fillna(method="ffill", inplace=True)

    # 4  clean description BEFORE classification
    df[1] = df[1].astype(str).apply(clean_description)

    # 5  amount (keep sign!)
    df[2] = (
        df[2]
        .astype(str)
        .str.replace("CHF", "", regex=False)
        .str.replace("'", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(" ", "", regex=False)
    )
    df[2] = pd.to_numeric(df[2], errors="coerce")

    # 6  classify → account
    df["account"] = df[1].apply(engine.classify).astype(str)

    # 7  Soll / Haben logic with sign
    def _soll(r):
        return r["account"] if r[2] < 0 else "1020"

    def _haben(r):
        return r["account"] if r[2] > 0 else "1020"

    df["soll"]  = df.apply(_soll,  axis=1)
    df["haben"] = df.apply(_haben, axis=1)

    # Store absolute amount for the ledger
    df[2] = df[2].abs()

    # 8  MWST flags
    df["MWST Code"]  = df["account"].apply(lambda a: "VB81" if a in _MWST_ACCOUNTS else "")
    df["MWST Konto"] = df["account"].apply(lambda a: a       if a in _MWST_ACCOUNTS else "")

    # 9  assemble tidy output
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

