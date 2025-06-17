"""
Streamlit front-end – v2
—————————
* lets the user pick **Bank** and **Client**
* loads the matching YAML from  ./configs/<client>.yaml
* routes the upload to the right parser:
      ─ PostFinance  → bookkeeping_app.py   (CSV)
      ─ Raiffeisen   → raiffeisen_transform (Excel)
"""

import io
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

# PostFinance toolkit (unchanged)
from bookkeeping_app import (
    read_bank_csv,
    normalise_columns,
    clean_description,
    KontierungEngine,
)

# NEW: Raiffeisen Excel pipeline
import raiffeisen_transform


# --------------------------------------------------------------------------- #
# UI SET-UP                                                                   #
# --------------------------------------------------------------------------- #
st.set_page_config(page_title="Bank ↦ Ledger", layout="centered")
st.title("Bank Statement → Ledger CSV")

BANKS = ["PostFinance", "Raiffeisen"]
CLIENTS = ["DB Financial", "Example AG", "Other Ltd"]

bank   = st.selectbox("Bank", BANKS, index=0)
client = st.selectbox("Client", CLIENTS, index=0)

# YAML is now fetched from a per-client file
cfg_path = Path("configs") / f"{client.lower().replace(' ', '_')}.yaml"

default_yaml = (
    cfg_path.read_text("utf-8") if cfg_path.exists()
    else "keywords:\n  \"coop|migros\": 4050\n"
)

yaml_text = st.text_area(
    "Keyword → Konto mapping (YAML)",
    value=default_yaml,
    height=180,
)

file_types = {"PostFinance": ["csv"], "Raiffeisen": ["xlsx", "xls"]}
uploader_label = f"Upload {bank} statement ({', '.join(file_types[bank])})"
data_file = st.file_uploader(uploader_label, type=file_types[bank])


# --------------------------------------------------------------------------- #
# PROCESSING                                                                  #
# --------------------------------------------------------------------------- #
if data_file and st.button("Process"):
    # -------------------------------------------------- 1 Parse YAML
    try:
        cfg = yaml.safe_load(yaml_text) or {}
        engine = KontierungEngine(cfg.get("keywords", {}))
    except yaml.YAMLError as err:
        st.error(f"YAML parsing error: {err}")
        st.stop()

    # -------------------------------------------------- 2 Bank-specific pipeline
    if bank == "PostFinance":
        # Persist the upload so existing CSV reader keeps working
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

        # Classification + Soll/Haben
        df["account"] = df["description"].apply(engine.classify)
        df["soll"]  = df.apply(lambda r: "1020" if r.amount > 0 else (r.account or ""), axis=1)
        df["haben"] = df.apply(lambda r: "1020" if r.amount < 0 else (r.account or ""), axis=1)

    else:   # ----------  Raiffeisen Excel  ----------
        try:
            df = raiffeisen_transform.process_excel(data_file, engine)
        except Exception as exc:
            st.error(f"❌ Failed to parse Excel: {exc}")
            st.stop()

    # -------------------------------------------------- 3 Preview & download
    st.subheader("Preview")
    st.dataframe(df.head(15), use_container_width=True)

    st.download_button(
        "Download ledger CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{Path(data_file.name).stem}_ledger.csv",
        mime="text/csv",
    )
