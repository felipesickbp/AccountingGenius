"""bookkeeping_app_streamlit.py – v0.3

A Streamlit front‑end that lets you upload Swiss bank‑statement CSVs, pick a client
(or create a new one on the fly) and download a tidy ledger that’s ready for
Bexio / Banana / Sage import.

Major features
--------------
* **Client chooser** with **“➕ Add new client”** button → creates an empty
  `<client>.yaml` in the `clients/` folder and refreshes the page.
* **CSV processor** re‑uses the exact rules from bookkeeping_app.py:
  – detects encoding, finds the real header row, normalises columns, cleans
    descriptions, classifies using YAML‐based keywords, generates *soll/haben*,
    etc.
* **Preview + download**: show the processed DataFrame and offer it as a
  CSV download.
* **Self‑contained**: drop it next to your CLI script; the two share the same
  helper functions so you don’t have to maintain duplicate logic.

Run it with:
    streamlit run bookkeeping_app_streamlit.py
"""
from __future__ import annotations

import csv
import hashlib
import io
import re
import textwrap
from datetime import date
from typing import Dict, List, Tuple, Optional
from pathlib import Path

import chardet
import pandas as pd
import streamlit as st
import yaml
from dateutil import parser as dateparser

# --------------------------------------------------------------------------- #
# Paths & configuration                                                       #
# --------------------------------------------------------------------------- #
BASE_DIR = Path(__file__).resolve().parent
CLIENT_DIR = BASE_DIR / "clients"
CLIENT_DIR.mkdir(exist_ok=True)

# --------------------------------------------------------------------------- #
# Helper – file‑name safe slug                                                #
# --------------------------------------------------------------------------- #

def slugify(name: str) -> str:
    """Convert arbitrary client names into safe file‑stems (ascii + underscores)."""
    name = name.strip().lower()
    replacements = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"}
    for src, tgt in replacements.items():
        name = name.replace(src, tgt)
    name = re.sub(r"\W+", "_", name)
    return name.strip("_")


# --------------------------------------------------------------------------- #
# CSV reading helpers (identical to CLI version)                              #
# --------------------------------------------------------------------------- #
HEADER_CANDIDATE = re.compile(r"^Datum;Buchungstext;Betrag;", re.I)
DATE_IN_TEXT = re.compile(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b")


def sniff_encoding(raw: bytes, sample_size: int = 16000) -> str:
    """Guess file encoding so pandas doesn’t choke on Windows‑encoded CSVs."""
    return chardet.detect(raw[:sample_size])["encoding"] or "utf-8"


def locate_header_row(lines: List[str]) -> int:
    """Return the 0‑based line number containing the real CSV header."""
    for idx, line in enumerate(lines):
        if HEADER_CANDIDATE.match(line):
            return idx
    raise ValueError("No header row with [Datum;Buchungstext;…] found")


def read_bank_csv(file: io.BytesIO) -> pd.DataFrame:
    """Read arbitrary Swiss bank statement CSV bytes into a DataFrame."""
    raw = file.read()
    encoding = sniff_encoding(raw)
    text = raw.decode(encoding, errors="ignore").splitlines()
    header_row = locate_header_row(text)
    # Re‑read via pandas from the same raw bytes
    df = pd.read_csv(
        io.BytesIO(raw),
        sep=";",
        encoding=encoding,
        header=0,
        skiprows=header_row,
        decimal=",",
    )
    return df


# --------------------------------------------------------------------------- #
# DataFrame normalisation & cleaning                                          #
# --------------------------------------------------------------------------- #

def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        re.compile(r"datum", re.I): "date",
        re.compile(r"buchungstext", re.I): "description",
        re.compile(r"betrag", re.I): "amount",
        re.compile(r"saldo", re.I): "balance",
        re.compile(r"valuta", re.I): "valuta",
        re.compile(r"währung|currency", re.I): "currency",
    }
    df = df.rename(
        columns={col: next((v for k, v in mapping.items() if k.search(col)), col) for col in df.columns}
    )
    return df


def clean_description(text: str) -> str:
    text = re.sub(r"Debit[- ]Einkauf", "", text, flags=re.I)
    text = re.sub(r"Mobile Banking[- ]Auftrag", "", text, flags=re.I)
    text = DATE_IN_TEXT.sub("", text)
    return " ".join(text.split()).strip()


# --------------------------------------------------------------------------- #
# Kontierung engine                                                           #
# --------------------------------------------------------------------------- #
class KontierungEngine:
    def __init__(self, keyword_map: Dict[str, str]):
        self.rules: List[Tuple[re.Pattern, str]] = [
            (re.compile(pat, re.I), acct) for pat, acct in keyword_map.items()
        ]

    def classify(self, description: str) -> Optional[str]:
        for pat, acct in self.rules:
            if pat.search(description):
                return acct
        return None


# --------------------------------------------------------------------------- #
# Client utilities                                                            #
# --------------------------------------------------------------------------- #

def list_clients() -> List[str]:
    return sorted(p.stem for p in CLIENT_DIR.glob("*.yaml"))


def create_client_file(name: str) -> None:
    safe = slugify(name)
    path = CLIENT_DIR / f"{safe}.yaml"
    if path.exists():
        raise FileExistsError("A client with that name already exists.")
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump({"keywords": {}}, fh, sort_keys=False)


def load_client_keywords(client: str) -> Dict[str, str]:
    path = CLIENT_DIR / f"{slugify(client)}.yaml"
    if not path.exists():
        st.warning(f"No YAML found for client '{client}'. Using empty rules.")
        return {}
    with path.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    return cfg.get("keywords", {})


# --------------------------------------------------------------------------- #
# Core processing logic (mirrors CLI)                                         #
# --------------------------------------------------------------------------- #

def process_ledger(df: pd.DataFrame, engine: KontierungEngine) -> pd.DataFrame:
    df = normalise_columns(df)

    # Clean & convert
    df["description"] = df["description"].astype(str).apply(clean_description)
    df["amount"] = (
        df["amount"].astype(str).str.replace("'", "").str.replace(",", ".").astype(float)
    )

    # Classify
    df["account"] = df["description"].apply(engine.classify)

    # Soll / Haben
    df["soll"] = df.apply(lambda r: "1020" if r["amount"] > 0 else (r["account"] or ""), axis=1)
    df["haben"] = df.apply(lambda r: "1020" if r["amount"] < 0 else (r["account"] or ""), axis=1)

    # Housekeeping
    df["date"] = pd.to_datetime(df["date"], dayfirst=True).dt.date
    df["needs_review"] = df["account"].isna()
    df["row_id"] = df.apply(lambda r: hashlib.sha1(str(r.values).encode()).hexdigest()[:10], axis=1)

    # Column order
    order = [
        "row_id",
        "date",
        "description",
        "amount",
        "currency",
        "soll",
        "haben",
        "needs_review",
        "balance",
        "valuta",
    ]
    df = df[[c for c in order if c in df.columns]]
    return df


# --------------------------------------------------------------------------- #
# Streamlit UI                                                                #
# --------------------------------------------------------------------------- #

def main():
    st.set_page_config("SME Bookkeeping Helper", page_icon="📒", layout="wide")
    st.title("📒 Swiss SME Bookkeeping Helper")
    st.caption("Upload a bank statement CSV → get a ready‑to‑import ledger.")

    # ---------------------------------------------------------------------- #
    # 1. Client selector + Add‑new button                                    #
    # ---------------------------------------------------------------------- #
    col_select, col_btn = st.columns([3, 1])

    with col_select:
        clients = list_clients()
        if clients:
            selected_client = st.selectbox("Client", clients, key="client_select")
        else:
            st.info("No clients yet – create one first.")
            selected_client = None

    with col_btn:
        if st.button("➕ Add new client", use_container_width=True):
            with st.modal("Create a new client"):
                st.markdown("Give your new client a name. A **.yaml** file will be created.")
                new_name = st.text_input("Client name", key="new_client_name")
                if st.button("Create", type="primary"):
                    if not new_name.strip():
                        st.warning("Please enter a non‑empty name.")
                        st.stop()
                    try:
                        create_client_file(new_name)
                        st.success(f"Client '{new_name}' created ✔")
                        st.experimental_rerun()
                    except FileExistsError as exc:
                        st.error(str(exc))
                        st.stop()

    st.divider()

    # ---------------------------------------------------------------------- #
    # 2. File uploader                                                       #
    # ---------------------------------------------------------------------- #
    uploaded_file = st.file_uploader("Bank statement CSV", type=["csv"], accept_multiple_files=False)
    preview = st.checkbox("Preview 10 rows after processing", value=True)

    # ---------------------------------------------------------------------- #
    # 3. Process button                                                      #
    # ---------------------------------------------------------------------- #
    if uploaded_file and selected_client:
        if st.button("🚀 Process file"):
            try:
                # Read + process
                df_raw = read_bank_csv(uploaded_file)
                keywords = load_client_keywords(selected_client)
                engine = KontierungEngine(keywords)
                df = process_ledger(df_raw, engine)

                # Preview
                if preview:
                    st.subheader("Preview")
                    st.dataframe(df.head(10), hide_index=True)

                # Download
                csv_bytes = df.to_csv(index=False, quoting=csv.QUOTE_NONNUMERIC).encode("utf-8-sig")
                st.download_button(
                    "⬇ Download processed ledger",
                    data=csv_bytes,
                    file_name=f"{Path(uploaded_file.name).stem}_ledger.csv",
                    mime="text/csv",
                )

                st.success("All done!")
            except Exception as exc:
                st.exception(exc)
    elif uploaded_file and not selected_client:
        st.warning("Please choose or create a client first.")


# --------------------------------------------------------------------------- #
# Script entry‑point                                                         #
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    main()

