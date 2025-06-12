import io, yaml, pandas as pd, streamlit as st
from pathlib import Path
from bookkeeping_app import (
    read_bank_csv, normalise_columns, clean_description, KontierungEngine
)

st.set_page_config(page_title="Bookkeeping", layout="centered")
st.title("📄 ➜ 💼  Bank CSV → Ledger")

csv_file = st.file_uploader("Drag a bank CSV here", type="csv")
yaml_text = st.text_area(
    "Keyword → Konto mapping (YAML)",
    Path("config.yaml").read_text() if Path("config.yaml").exists() else
    "keywords:\n  \"coop|migros\": 4050 Lebensmittel\n  \"sbb\": 6850 Reisekosten\n",
    height=180,
)

if csv_file and st.button("Process"):
    cfg = yaml.safe_load(yaml_text) or {}
    engine = KontierungEngine(cfg.get("keywords", {}))

    df = read_bank_csv(io.BytesIO(csv_file.getvalue()))
    df = normalise_columns(df)
    df["description"] = df["description"].astype(str).apply(clean_description)
    df["amount"] = df["amount"].astype(str).str.replace("'", "").str.replace(",", ".").astype(float)
    df["account"] = df["description"].apply(engine.classify)
    df["soll"]  = df.apply(lambda r: "1020" if r.amount > 0 else (r.account or ""), axis=1)
    df["haben"] = df.apply(lambda r: "1020" if r.amount < 0 else (r.account or ""), axis=1)

    st.subheader("Preview")
    st.dataframe(df.head(15), use_container_width=True)

    st.download_button(
        "Download ledger CSV",
        df.to_csv(index=False).encode(),
        file_name=f"{Path(csv_file.name).stem}_ledger.csv",
    )
