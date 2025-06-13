"""Streamlit front‑end for bookkeeping_app.py

This version writes the uploaded CSV to a temporary file so the existing
read_bank_csv(pathlib.Path) function keeps working.
"""
import io
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


st.set_page_config(page_title="Bookkeeping", layout="centered")
st.title("Bank CSV → Ledger")

# ----------------------------------------------------------------------------
# Widgets
# ----------------------------------------------------------------------------
csv_file = st.file_uploader("Drag a bank CSV here", type="csv")

default_yaml = (
    Path("config.yaml").read_text(encoding="utf-8")
    if Path("config.yaml").exists()
    else "keywords:\n  \"coop|migros\": 4050 Lebensmittel\n  \"sbb\": 6850 Reisekosten\n"
)
yaml_text = st.text_area(
    "Keyword → Konto mapping (YAML)", value=default_yaml, height=180
)

# ----------------------------------------------------------------------------
# Processing on button click
# ----------------------------------------------------------------------------
if csv_file and st.button("Process"):
    # 1. Load YAML rules
    try:
        cfg = yaml.safe_load(yaml_text) or {}
        engine = KontierungEngine(cfg.get("keywords", {}))
    except yaml.YAMLError as err:
        st.error(f"YAML parsing error: {err}")
        st.stop()

    # 2. Persist upload to a temp file so read_bank_csv can work unchanged
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(csv_file.getvalue())
        tmp_path = Path(tmp.name)

    # 3. Run the pipeline
    try:
        df = read_bank_csv(tmp_path)
    except Exception as e:
        st.error(f"❌ Failed to read CSV: {e}")
        st.stop()

    df = normalise_columns(df)
    df["description"] = df["description"].astype(str).apply(clean_description)
    df["amount"] = (
        df["amount"].astype(str).str.replace("'", "").str.replace(",", ".").astype(float)
    )

    # 4. Classification + Soll/Haben
    df["account"] = df["description"].apply(engine.classify)
    df["soll"] = df.apply(lambda r: "1020" if r.amount > 0 else (r.account or ""), axis=1)
    df["haben"] = df.apply(lambda r: "1020" if r.amount < 0 else (r.account or ""), axis=1)

    # 5. Preview
    st.subheader("Preview")
    st.dataframe(df.head(15), use_container_width=True)

    # 6. Download button
    st.download_button(
        "Download ledger CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{Path(csv_file.name).stem}_ledger.csv",
        mime="text/csv",
    )
