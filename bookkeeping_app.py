

"""bookkeeping_app.py – v0.2

Improvements
============
* Robustly finds the *real* header row (e.g. when Swiss QR-bank statements
  prepend 10 lines of metadata) and skips everything before it.
* Cleans the description column by removing:
    – the prefixes "Debit-Einkauf" and "Mobile Banking-Auftrag"
    – embedded date fragments like “25.03.2025”
* Adds two booking-friendly columns **soll** and **haben**:
    – If *amount* is **positive** → *soll* = 1020, *haben* = classified account
    – If *amount* is **negative** → *haben* = 1020, *soll* = classified account
* Keeps the YAML-based keyword→account engine so you can grow your rules.
* Still exports a tidy CSV ready for import into banana, bexio, Sage, etc.
"""
from __future__ import annotations

import re
import sys
import csv
import hashlib
import pathlib
from typing import Dict, List, Tuple, Optional

import click
import pandas as pd
import yaml
from dateutil import parser as dateparser
import chardet


# --------------------------------------------------------------------------- #
# Utility helpers                                                             #
# --------------------------------------------------------------------------- #
HEADER_CANDIDATE = re.compile(r"^Datum;Buchungstext;Betrag;", re.I)
DATE_IN_TEXT = re.compile(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b")


def sniff_encoding(path: pathlib.Path, sample_size: int = 16000) -> str:
    """Detect encoding so the parser doesn't break on Windows-encoded CSVs."""
    with path.open("rb") as fh:
        raw = fh.read(sample_size)
    return chardet.detect(raw)["encoding"] or "utf-8"


def locate_header_row(path: pathlib.Path, encoding: str) -> int:
    """Return the 0-based line number that contains the real CSV header."""
    with path.open("r", encoding=encoding, errors="ignore") as fh:
        for idx, line in enumerate(fh):
            if HEADER_CANDIDATE.match(line):
                return idx
    raise ValueError("No header row with [Datum;Buchungstext;…] found")


def read_bank_csv(path: pathlib.Path) -> pd.DataFrame:
    """Read arbitrary Swiss statement CSVs (UBS, CS, etc.) into a DataFrame."""
    encoding = sniff_encoding(path)
    header_row = locate_header_row(path, encoding)
    df = pd.read_csv(
        path,
        sep=";",
        encoding=encoding,
        header=0,
        skiprows=header_row,
        decimal=",",  # some banks use commas; pandas fixes later
    )
    return df


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        re.compile(r"datum", re.I): "date",
        re.compile(r"buchungstext", re.I): "description",
        re.compile(r"betrag", re.I): "amount",
        re.compile(r"saldo", re.I): "balance",
        re.compile(r"valuta", re.I): "valuta",
    }
    df = df.rename(
        columns={
            col: next((v for k, v in mapping.items() if k.search(col)), col)
            for col in df.columns
        }
    )
    return df


def clean_description(text: str) -> str:
    text = re.sub(r"Debit[- ]Einkauf", "", text, flags=re.I)
    text = re.sub(r"Mobile Banking[- ]Auftrag", "", text, flags=re.I)
    text = DATE_IN_TEXT.sub("", text)  # remove embedded dates
    return " ".join(text.split()).strip()  # compress whitespace


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
# CLI                                                                         #
# --------------------------------------------------------------------------- #
@click.group()
def cli():
    """Automate Swiss SME bookkeeping from messy bank CSVs."""


@cli.command("process")
@click.argument("csv_paths", nargs=-1, type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    help="YAML file with 'keywords' mapping.",
)
@click.option(
    "--out",
    "out_dir",
    type=click.Path(file_okay=False),
    required=True,
    help="Destination folder for processed ledgers.",
)
@click.option("--preview/--no-preview", default=False)
def process_cmd(csv_paths: List[str], config_path: str, out_dir: str, preview: bool):
    """Convert CSV(s) into a normalised ledger with Soll/Haben split."""
    out_path = pathlib.Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    with open(config_path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    engine = KontierungEngine(cfg.get("keywords", {}))

    for p in csv_paths:
        path = pathlib.Path(p)
        click.echo(click.style(f"→ {path.name}", bold=True))
        df = read_bank_csv(path)
        df = normalise_columns(df)

        # Clean up
        df["description"] = df["description"].astype(str).apply(clean_description)
        df["amount"] = (
            df["amount"]
            .astype(str)
            .str.replace("'", "")
            .str.replace(",", ".")
            .astype(float)
        )

        # Classification
        df["account"] = df["description"].apply(engine.classify)

        # Soll / Haben logic
        df["soll"] = df.apply(
            lambda r: "1020" if r["amount"] > 0 else (r["account"] or ""), axis=1
        )
        df["haben"] = df.apply(
            lambda r: "1020" if r["amount"] < 0 else (r["account"] or ""), axis=1
        )

        # housekeeping
        df["date"] = pd.to_datetime(df["date"], dayfirst=True).dt.date
        df["needs_review"] = df["account"].isna()
        df["row_id"] = df.apply(
            lambda r: hashlib.sha1(str(r.values).encode()).hexdigest()[:10], axis=1
        )

        # Column order
        order = [
            "row_id",
            "date",
            "description",
            "amount",
            "currency",  # may not exist
            "soll",
            "haben",
            "needs_review",
            "balance",
            "valuta",
        ]
        df = df[[c for c in order if c in df.columns]]

        if preview:
            click.echo(df.head().to_string(index=False))
        else:
            outfile = out_path / f"{path.stem}_ledger.csv"
            df.to_csv(outfile, index=False, quoting=csv.QUOTE_NONNUMERIC)
            click.echo(click.style(f"   ✓ wrote {outfile}", fg="green"))


if __name__ == "__main__":
    try:
        cli()
    except BrokenPipeError:
        # Don't crash if piped into `head`
        sys.stderr.close()
