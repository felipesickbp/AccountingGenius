"""
Streamlit front-end – v3
—————————
* PostFinance  → bookkeeping_app.py   (CSV)
* Raiffeisen   → raiffeisen_transform (Excel)
* Adds running Belegnummer, MWST Code & MWST Konto
"""

from __future__ import annotations
import io
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

# --------------------------------------------------------------------------- #
#  back-end helpers                                                           #
# --------------------------------------------------------------------------- #
from bookkeeping_app import (
    read_bank_csv,
    normalise_columns,
    clean_description,
    KontierungEngine,
)
import raiffeisen_transform

# --------------------------------------------------------------------------- #
#  UI                                                                          #
# --------------------------------------------------------------------------- #
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

# Load / show YAML
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

file_types      = {"PostFinance": ["csv"], "Raiffeisen": ["xlsx", "xls"]}
uploader_label  = f"Upload {bank} statement ({', '.join(file_types[bank])})"
data_file       = st.file_uploader(uploader_label, type=file_types[bank])

# --------------------------------------------------------------------------- #
#  Process                                                                    #
# --------------------------------------------------------------------------- #
if data_file and st.button("Process"):
    # -- 1  YAML  ----------------------------------------------------------- #
    try:
        cfg = yaml.safe_load(yaml_text) or {}
        # cast every Konto to string → prevents 6530.0
        if "keywords" in cfg:
            cfg["keywords"] = {pat: str(acct) for pat, acct in cfg["keywords"].items()}
        engine = KontierungEngine(cfg.get("keywords", {}))
    except yaml.YAMLError as err:
        st.error(f"YAML parsing error: {err}")
        st.stop()

    # -- 2  Parse statement ------------------------------------------------- #
    if bank == "PostFinance":
        # write temp file so legacy CSV reader can open it
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

        # date to Swiss format
        df["date"] = pd.to_datetime(df["date"], dayfirst=True).dt.strftime("%d.%m.%Y")

        # classification
        df["account"] = df["description"].apply(engine.classify).astype(str)

        def _soll(r):  # strings only
            return "1020" if r.amount > 0 else (r.account if r.account != "None" else "")

        def _haben(r):
            return "1020" if r.amount < 0 else (r.account if r.account != "None" else "")

        df["soll"]  = df.apply(_soll,  axis=1)
        df["haben"] = df.apply(_haben, axis=1)

        df = df[["date", "description", "amount", "soll", "haben", "account"]]

    else:  # ---------------- Raiffeisen Excel ----------------------------- #
        try:
            df = raiffeisen_transform.process_excel(data_file, engine, start_no=start_no)
        except Exception as exc:
            st.error(f"❌ Failed to parse Excel: {exc}")
            st.stop()

    # -- 3  Ensure template columns ---------------------------------------- #
    if bank == "PostFinance":
        MWST_ACCOUNTS = {"6210", "6260", "6510", "6530", "6640"}

        df["MWST Code"]  = df["account"].apply(lambda a: "VB81" if a in MWST_ACCOUNTS else "")
        df["MWST Konto"] = df["account"].apply(lambda a: a      if a in MWST_ACCOUNTS else "")

        df.insert(0, "Belegnummer", range(int(start_no), int(start_no) + len(df)))
        df["Währung"]     = "CHF"
        df["Wechselkurs"] = ""

        df = df.rename(
            columns={
                "date":        "Datum",
                "description": "Beschreibung",
                "amount":      "Betrag",
                "soll":        "Soll",
                "haben":       "Haben",
            }
        )

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

    else:
        # raiffeisen_transform already returns final columns; just re-order to be safe
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

    # -- 4  Preview & download --------------------------------------------- #
    st.subheader("Preview")
    st.dataframe(df.head(15), use_container_width=True)

    st.download_button(
        "Download ledger CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{Path(data_file.name).stem}_ledger.csv",
        mime="text/csv",
    )
