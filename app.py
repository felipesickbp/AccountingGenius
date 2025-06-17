"""
Streamlit front-end – v3.3
——————————
* Same features as v3.2
* Fixes ValueError when “Belegnummer” already exists
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
    "Keyword → Konto mapping (YAML)", value=default_yaml, height=180
)

file_types = {"PostFinance": ["csv"], "Raiffeisen": ["xlsx", "xls"]}
data_file  = st.file_uploader(
    f"Upload {bank} statement ({', '.join(file_types[bank])})",
    type=file_types[bank],
)

# ──────────────────────────────────────────────────────────────────────────────
# Helper
# ──────────────────────────────────────────────────────────────────────────────
TEMPLATE_ORDER = [
    "Belegnummer", "Datum", "Beschreibung", "Betrag", "Währung", "Wechselkurs",
    "Soll", "Haben", "MWST Code", "MWST Konto",
]

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



def finalise(df: pd.DataFrame, first_no: int) -> pd.DataFrame:
    """Rename / add columns until they match TEMPLATE_ORDER."""
    # English → German
    df = df.rename(columns={
        "date": "Datum",
        "description": "Beschreibung",
        "amount": "Betrag",
        "soll": "Soll",
        "haben": "Haben",
    })

    # Ensure Soll/Haben are strings
    for col in ("Soll", "Haben"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Belegnummer (overwrite or create, then move to front later)
    df["Belegnummer"] = range(int(first_no), int(first_no) + len(df))

    # Währung & Wechselkurs
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

    # Canonical order
    for col in TEMPLATE_ORDER:
        if col not in df.columns:
            df[col] = ""
    return df[TEMPLATE_ORDER]


# ──────────────────────────────────────────────────────────────────────────────
# Main button
# ──────────────────────────────────────────────────────────────────────────────
if data_file and st.button("Process"):
    # YAML → mapping
    try:
        cfg = yaml.safe_load(yaml_text) or {}
        if "keywords" in cfg:
            cfg["keywords"] = {pat: str(acct) for pat, acct in cfg["keywords"].items()}
        engine = KontierungEngine(cfg.get("keywords", {}))
    except yaml.YAMLError as err:
        st.error(f"YAML error: {err}")
        st.stop()

    # Import -----------------------------------------------------------------
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
            df["amount"].astype(str).str.replace("'", "").str.replace(",", ".").astype(float)
        )
        df["date"] = pd.to_datetime(df["date"], dayfirst=True).dt.strftime("%d.%m.%Y")
        df["account"] = df["description"].apply(engine.classify).astype(str)

        df["soll"]  = df.apply(
            lambda r: "1020" if r.amount > 0 else (r.account or ""), axis=1
        )
        df["haben"] = df.apply(
            lambda r: "1020" if r.amount < 0 else (r.account or ""), axis=1
        )
        df = df[["date", "description", "amount", "soll", "haben"]]

    else:  # Raiffeisen Excel
        try:
            df = raiffeisen_transform.process_excel(data_file, engine, start_no=start_no)
        except Exception as exc:
            st.error(f"❌ Failed to parse Excel: {exc}")
            st.stop()

    # Template & export ------------------------------------------------------
    df = finalise(df, start_no)

    st.subheader("Preview")
    st.dataframe(df.head(15), use_container_width=True)

    st.download_button(
        "Download ledger CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{Path(data_file.name).stem}_ledger.csv",
        mime="text/csv",
    )

