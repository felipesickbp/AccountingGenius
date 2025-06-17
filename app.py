"""
Streamlit front-end – v3.2
——————————
* Bank / Client selectors
* Belegnummer-Start widget
* YAML Konto values coerced to strings
* Robust final-column builder
"""

from __future__ import annotations
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

from bookkeeping_app import (
    read_bank_csv,
    normalise_columns,
    clean_description,
    KontierungEngine,
)
import raiffeisen_transform


# ──────────────────────────────────────────────────────────────────────────────
# UI
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Bank ↦ Ledger", layout="centered")
st.title("Bank Statement → Ledger CSV")

BANKS   = ["PostFinance", "Raiffeisen"]
CLIENTS = ["DB Financial", "Example AG", "Other Ltd"]

bank   = st.selectbox("Bank", BANKS,   index=0)
client = st.selectbox("Client", CLIENTS, index=0)

start_no = st.number_input(
    "First voucher number (Belegnummer-Start)",
    min_value=1,
    value=1,
    step=1,
)

cfg_path = Path("configs") / f"{client.lower().replace(' ', '_')}.yaml"
default_yaml = (
    cfg_path.read_text("utf-8")
    if cfg_path.exists()
    else "keywords:\n  \"coop|migros\": 4050\n"
)
yaml_text = st.text_area(
    "Keyword → Konto mapping (YAML)",
    value=default_yaml,
    height=180,
)

file_types = {"PostFinance": ["csv"], "Raiffeisen": ["xlsx", "xls"]}
data_file  = st.file_uploader(
    f"Upload {bank} statement ({', '.join(file_types[bank])})",
    type=file_types[bank],
)

# ──────────────────────────────────────────────────────────────────────────────
# Helper: build the 10-column template
# ──────────────────────────────────────────────────────────────────────────────
TEMPLATE_ORDER = [
    "Belegnummer", "Datum", "Beschreibung", "Betrag", "Währung", "Wechselkurs",
    "Soll", "Haben", "MWST Code", "MWST Konto",
]
MWST_ACCOUNTS = {"6210", "6260", "6510", "6530", "6640"}


def finalise(df: pd.DataFrame, first_no: int) -> pd.DataFrame:
    """Rename / add columns until they match TEMPLATE_ORDER."""
    df = df.rename(columns={
        "date": "Datum",
        "description": "Beschreibung",
        "amount": "Betrag",
        "soll": "Soll",
        "haben": "Haben",
    })

    # Soll / Haben must be strings
    for col in ("Soll", "Haben"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Belegnummer
    if "Belegnummer" not in df.columns:
        df.insert(0, "Belegnummer", range(int(first_no), int(first_no) + len(df)))

    # Währung & Wechselkurs  – FIX (no DataFrame.setdefault)
    if "Währung" not in df.columns:
        df["Währung"] = "CHF"
    if "Wechselkurs" not in df.columns:
        df["Wechselkurs"] = ""

    # MWST columns
    if {"MWST Code", "MWST Konto"}.issubset(df.columns) is False:
        df["MWST Code"]  = ""
        df["MWST Konto"] = ""
        mask = df["Soll"].isin(MWST_ACCOUNTS) | df["Haben"].isin(MWST_ACCOUNTS)
        df.loc[mask, "MWST Code"]  = "VB81"
        df.loc[mask, "MWST Konto"] = (
            df.loc[mask, ["Soll", "Haben"]].bfill(axis=1).iloc[:, 0]
        )

    # Ensure canonical order
    for col in TEMPLATE_ORDER:
        if col not in df.columns:
            df[col] = ""
    return df[TEMPLATE_ORDER]


# ──────────────────────────────────────────────────────────────────────────────
#  Main button — parse & transform
# ──────────────────────────────────────────────────────────────────────────────
if data_file and st.button("Process"):
    # 1 YAML → mapping
    try:
        cfg = yaml.safe_load(yaml_text) or {}
        if "keywords" in cfg:
            cfg["keywords"] = {pat: str(acct) for pat, acct in cfg["keywords"].items()}
        engine = KontierungEngine(cfg.get("keywords", {}))
    except yaml.YAMLError as err:
        st.error(f"YAML parsing error: {err}")
        st.stop()

    # 2 Import
    if bank == "PostFinance":
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(data_file.getvalue())
            tmp_path = Path(tmp.name)

        try:
            df = read_bank_csv(tmp_path)
        except Exception as exc:
            st.error(f"❌ Failed to read CSV: {exc}")
            st.stop()

        df = normalise_columns(df)
        df["description"] = df["description"].astype(str).apply(clean_description)
        df["amount"] = (
            df["amount"].astype(str)
            .str.replace("'", "")
            .str.replace(",", ".")
            .astype(float)
        )
        df["date"] = pd.to_datetime(df["date"], dayfirst=True).dt.strftime("%d.%m.%Y")
        df["account"] = df["description"].apply(engine.classify).astype(str)

        def _soll(r):  # strings only
            return "1020" if r.amount > 0 else (r.account if r.account != "None" else "")

        def _haben(r):
            return "1020" if r.amount < 0 else (r.account if r.account != "None" else "")

        df["soll"]  = df.apply(_soll,  axis=1)
        df["haben"] = df.apply(_haben, axis=1)
        df = df[["date", "description", "amount", "soll", "haben"]]

    else:  # Raiffeisen
        try:
            df = raiffeisen_transform.process_excel(data_file, engine, start_no=start_no)
        except Exception as exc:
            st.error(f"❌ Failed to parse Excel: {exc}")
            st.stop()

    # 3 Template & export
    df = finalise(df, start_no)

    st.subheader("Preview")
    st.dataframe(df.head(15), use_container_width=True)

    st.download_button(
        "Download ledger CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{Path(data_file.name).stem}_ledger.csv",
        mime="text/csv",
    )
