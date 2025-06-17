"""
Streamlit front-end – v3
—————————
* Bank & Client selectors
* Per-client YAML loaded from ./configs/<client>.yaml
* Voucher-number start (Belegnummer-Start) widget
* Two back-ends:
      ─ PostFinance  → bookkeeping_app.py   (CSV)
      ─ Raiffeisen   → raiffeisen_transform (Excel)
* Final export matches the 10-column template:
    Belegnummer | Datum | Beschreibung | Betrag | Währung | Wechselkurs
    | Soll | Haben | MWST Code | MWST Konto
"""

from __future__ import annotations

import io
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

# --------------------------------------------------------------------------- #
#  Back-end helpers                                                           #
# --------------------------------------------------------------------------- #
from bookkeeping_app import (
    read_bank_csv,
    normalise_columns,
    clean_description,
    KontierungEngine,
)

import raiffeisen_transform  # Excel parser for Raiffeisen

# --------------------------------------------------------------------------- #
#  UI – static                                                                #
# --------------------------------------------------------------------------- #
st.set_page_config(page_title="Bank ↦ Ledger", layout="centered")
st.title("Bank Statement → Ledger CSV")

BANKS   = ["PostFinance", "Raiffeisen"]
CLIENTS = ["DB Financial", "Example AG", "Other Ltd"]

bank   = st.selectbox("Bank",   BANKS,   index=0)
client = st.selectbox("Client", CLIENTS, index=0)

# Belegnummer start widget
start_no = st.number_input(
    "First voucher number (Belegnummer-Start)",
    min_value=1,
    value=1,
    step=1,
)

# YAML path per client
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

# File-uploader (bank-specific extensions)
file_types      = {"PostFinance": ["csv"], "Raiffeisen": ["xlsx", "xls"]}
uploader_label  = f"Upload {bank} statement ({', '.join(file_types[bank])})"
data_file       = st.file_uploader(uploader_label, type=file_types[bank])

# --------------------------------------------------------------------------- #
#  Process on click                                                           #
# --------------------------------------------------------------------------- #
if data_file and st.button("Process"):
    # ------------------------------------------------ 1  YAML rules
    try:
        cfg     = yaml.safe_load(yaml_text) or {}
        engine  = KontierungEngine(cfg.get("keywords", {}))
    except yaml.YAMLError as err:
        st.error(f"YAML parsing error: {err}")
        st.stop()

    # ------------------------------------------------ 2  Parse statement
    if bank == "PostFinance":
        # Write to a temporary CSV file (keeps legacy reader happy)
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
            df["amount"]
            .astype(str)
            .str.replace("'", "")
            .str.replace(",", ".")
            .astype(float)
        )

        # Classification & Soll/Haben
        df["account"] = df["description"].apply(engine.classify)
        df["soll"]   = df.apply(lambda r: "1020" if r.amount > 0 else (r.account or ""), axis=1)
        df["haben"]  = df.apply(lambda r: "1020" if r.amount < 0 else (r.account or ""), axis=1)

        # Normalise date (dd.mm.yyyy)
        df["date"] = pd.to_datetime(df["date"], dayfirst=True).dt.strftime("%d.%m.%Y")

        df = df[["date", "description", "amount", "soll", "haben"]]

    else:  # ---------------- Raiffeisen Excel ----------------
        try:
            df = raiffeisen_transform.process_excel(data_file, engine)
        except Exception as exc:
            st.error(f"❌ Failed to parse Excel: {exc}")
            st.stop()

    # ------------------------------------------------ 3  Shape to 10-column template
    df = df.rename(
        columns={
            "date":        "Datum",
            "description": "Beschreibung",
            "amount":      "Betrag",
            "soll":        "Soll",
            "haben":       "Haben",
        }
    )

    df["Währung"]     = "CHF"
    df["Wechselkurs"] = ""   # CHF = base → leave empty

    MWST_ACCOUNTS = {"6210", "6260", "6510", "6530", "6640"}

    def mwst_code(row):
        acc = str(row["Soll"] or row["Haben"]).strip()
        return "MWST" if acc in MWST_ACCOUNTS else ""

    def mwst_konto(row):
        acc = str(row["Soll"] or row["Haben"]).strip()
        return acc if acc in MWST_ACCOUNTS else ""

    df["MWST Code"]  = df.apply(mwst_code,  axis=1)
    df["MWST Konto"] = df.apply(mwst_konto, axis=1)

    # Voucher numbers
    df.insert(
        0,
        "Belegnummer",
        range(int(start_no), int(start_no) + len(df)),
    )

    # Final column order
    order = [
        "Belegnummer",
        "Datum",
        "Beschreibung",
        "Betrag",
        "Währung",
        "Wechselkurs",
        "Soll",
        "Haben",
        "MWST Code",
        "MWST Konto",
    ]
    df = df[order]

    # ------------------------------------------------ 4  Preview & download
    st.subheader("Preview")
    st.dataframe(df.head(15), use_container_width=True)

    st.download_button(
        "Download ledger CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{Path(data_file.name).stem}_ledger.csv",
        mime="text/csv",
    )
