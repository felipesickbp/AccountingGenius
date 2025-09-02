"""
Streamlit front-end – v3.5
——————————
• Editable grid works directly on the processed ledger (no second upload)
• Optional loader to swap in a different CSV/XLSX for the editor
• OAuth migrated to auth.bexio.com via OIDC discovery
• Uses st.query_params (no experimental deprecation)
• Post edited/processed rows to bexio (dry-run by default)

Security
--------
Set secrets via env/Streamlit secrets:
  BEXIO_CLIENT_ID, BEXIO_CLIENT_SECRET, BEXIO_REDIRECT_URI, (optional) BEXIO_SCOPES
Do NOT hardcode real secrets in code for production.
"""
from __future__ import annotations

import io
import json
import os
import time
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
import yaml

# Optional: import your existing helpers (safe fallback if not present)
try:
    from bookkeeping_app import (
        read_bank_csv,
        normalise_columns,
        clean_description,
        KontierungEngine,
    )
    import raiffeisen_transform
except Exception:
    read_bank_csv = normalise_columns = clean_description = KontierungEngine = None  # type: ignore
    raiffeisen_transform = None  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# OAuth / API config (auth.bexio.com + OIDC discovery)
# ──────────────────────────────────────────────────────────────────────────────

# Helper: prefer Streamlit Secrets, then env vars, then explicit fallback
def _get(name: str, default: Optional[str] = None):
    return st.secrets.get(name, os.getenv(name, default))

# === Put YOUR real keys/URL here (or set them in Streamlit Secrets) ===
HARDCODED_CLIENT_ID     = "9a1bbd33-58d9-48e6-8f87-d938d0f5215e"
HARDCODED_CLIENT_SECRET = "B_d-wAUgzALrsYYyoR2-rv1df1X1mPNxlnDIKhuHM3yeke2h38CKTi2DIXeLSjkseTGXw3wxP0PISRryl_8KTQ"
HARDCODED_REDIRECT_URI  = "https://accountinggenius-mdcq7sh8scyxglc7vvwcuh.streamlit.app"  # must match bexio app exactly

# Resolved config (Secrets/env take precedence, else hardcoded)
CLIENT_ID     = _get("BEXIO_CLIENT_ID",     HARDCODED_CLIENT_ID)
CLIENT_SECRET = _get("BEXIO_CLIENT_SECRET", HARDCODED_CLIENT_SECRET)
REDIRECT_URI  = _get("BEXIO_REDIRECT_URI",  HARDCODED_REDIRECT_URI)

SCOPES = _get(
    "BEXIO_SCOPES",
    "openid profile email offline_access",
)


# Fail fast if empty or still placeholders
if any(x in (None, "", "MY_CLIENT_ID_HERE", "MY_SECRET_KEY_HERE") for x in (CLIENT_ID, CLIENT_SECRET)):
    st.error("Missing BEXIO_CLIENT_ID / BEXIO_CLIENT_SECRET. Fill the HARDCODED_* values or set Streamlit secrets.")
    st.stop()

# OIDC discovery on the current issuer (https://auth.bexio.com)
OIDC_ISSUER = _get("BEXIO_OIDC_ISSUER", "https://auth.bexio.com")
DISCOVERY_URL = f"{OIDC_ISSUER}/.well-known/openid-configuration"


@st.cache_data(ttl=3600, show_spinner=False)
def _discover_oidc() -> Dict[str, str]:
    try:
        r = requests.get(DISCOVERY_URL, timeout=30)
        if r.ok:
            return r.json()
    except Exception:
        pass
    # fallback if discovery is unavailable
    return {
        "authorization_endpoint": f"{OIDC_ISSUER}/authorize",
        "token_endpoint":         f"{OIDC_ISSUER}/token",
        "userinfo_endpoint":      f"{OIDC_ISSUER}/userinfo",
        "issuer":                  OIDC_ISSUER,
    }

_oidc        = _discover_oidc()
AUTH_URL     = _oidc.get("authorization_endpoint")
TOKEN_URL    = _oidc.get("token_endpoint")
USERINFO_URL = _oidc.get("userinfo_endpoint")
ISSUER       = _oidc.get("issuer", OIDC_ISSUER)

API_BASE = _get("BEXIO_API_BASE", "https://api.bexio.com/2.0")
MANUAL_ENTRY_ENDPOINT = _get("BEXIO_MANUAL_ENTRY_ENDPOINT", "/accounting/manual_entries")



# ──────────────────────────────────────────────────────────────────────────────
# Streamlit page + state
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Bank ↦ Ledger (+ Excel editor + bexio)", layout="wide")
st.title("Bank Statement → Ledger CSV · Excel editor · bexio posting")


# init editor/posting state
if "ledger_df" not in st.session_state:
    st.session_state["ledger_df"] = None
if "editor_df" not in st.session_state:
    st.session_state["editor_df"] = None


BANKS   = ["PostFinance", "Raiffeisen"]
CLIENTS = ["DB Financial", "Example AG", "Other Ltd"]

left, right = st.columns([1, 1])
with left:
    bank = st.selectbox("Bank", BANKS, index=0)
with right:
    client = st.selectbox("Client (local profile)", CLIENTS, index=0)

start_no = st.number_input("First voucher number (Belegnummer-Start)", min_value=1, value=1, step=1)

# Keyword→Konto YAML
cfg_path = Path("configs") / f"{client.lower().replace(' ', '_')}.yaml"
default_yaml = cfg_path.read_text("utf-8") if cfg_path.exists() else 'keywords:\n  "coop|migros": 4050\n'
yaml_text = st.text_area("Keyword → Konto mapping (YAML)", value=default_yaml, height=160)

file_types = {"PostFinance": ["csv"], "Raiffeisen": ["xlsx", "xls"]}
data_file  = st.file_uploader(f"Upload {bank} statement ({', '.join(file_types[bank])})",
                              type=file_types[bank])

# ──────────────────────────────────────────────────────────────────────────────
# Template & VAT helpers
# ──────────────────────────────────────────────────────────────────────────────
TEMPLATE_ORDER = [
    "Belegnummer", "Datum", "Beschreibung", "Betrag", "Währung", "Wechselkurs",
    "Soll", "Haben", "MWST Code", "MWST Konto",
]

_MWST_ACCOUNTS = {
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

def finalise(df: pd.DataFrame, first_no: int) -> pd.DataFrame:
    """Rename / add columns until they match TEMPLATE_ORDER."""
    df = df.rename(columns={
        "date": "Datum",
        "description": "Beschreibung",
        "amount": "Betrag",
        "soll": "Soll",
        "haben": "Haben",
    })
    for col in ("Soll", "Haben"):
        if col in df.columns:
            df[col] = df[col].astype(str)
    df["Belegnummer"] = range(int(first_no), int(first_no) + len(df))
    if "Währung" not in df.columns:
        df["Währung"] = "CHF"
    if "Wechselkurs" not in df.columns:
        df["Wechselkurs"] = ""
    if {"MWST Code", "MWST Konto"}.issubset(df.columns) is False:
        df["MWST Code"]  = ""
        df["MWST Konto"] = ""
    mask = df["Soll"].isin(_MWST_ACCOUNTS) | df["Haben"].isin(_MWST_ACCOUNTS)
    df.loc[mask, "MWST Code"]  = df.loc[mask, "MWST Code"].replace("", "VB81")
    df.loc[mask, "MWST Konto"] = df.loc[mask, ["Soll", "Haben"]].bfill(axis=1).iloc[:, 0]
    for col in TEMPLATE_ORDER:
        if col not in df.columns:
            df[col] = ""
    return df[TEMPLATE_ORDER]

# ──────────────────────────────────────────────────────────────────────────────
# Import + Process → directly feed the editor
# ──────────────────────────────────────────────────────────────────────────────
if data_file and st.button("Process bank statement → ledger"):
    try:
        cfg = yaml.safe_load(yaml_text) or {}
        if "keywords" in cfg:
            cfg["keywords"] = {pat: str(acct) for pat, acct in cfg["keywords"].items()}
        engine = KontierungEngine(cfg.get("keywords", {})) if KontierungEngine else None
    except yaml.YAMLError as err:
        st.error(f"YAML error: {err}")
        st.stop()

    if bank == "PostFinance" and read_bank_csv is not None:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(data_file.getvalue())
            tmp_path = Path(tmp.name)
        try:
            df = read_bank_csv(tmp_path)
        except Exception as exc:
            st.error(f"❌ Failed to read CSV: {exc}")
            st.stop()
        df = normalise_columns(df) if normalise_columns else df
        if clean_description:
            df["description"] = df["description"].astype(str).apply(clean_description)
        df["amount"] = df["amount"].astype(str).str.replace("'", "").str.replace(",", ".").astype(float)
        df["date"] = pd.to_datetime(df["date"], dayfirst=True).dt.strftime("%d.%m.%Y")
        if engine:
            df["account"] = df["description"].apply(engine.classify).astype(str)
        else:
            df["account"] = ""
        df["soll"]  = df.apply(lambda r: "1020" if r.amount > 0 else (r.account or ""), axis=1)
        df["haben"] = df.apply(lambda r: "1020" if r.amount < 0 else (r.account or ""), axis=1)
        df = df[["date", "description", "amount", "soll", "haben"]]
    else:
        if raiffeisen_transform is None:
            st.error("The 'raiffeisen_transform' module is not available.")
            st.stop()
        try:
            df = raiffeisen_transform.process_excel(data_file, engine, start_no=start_no)
        except Exception as exc:
            st.error(f"❌ Failed to parse Excel: {exc}")
            st.stop()

    df = finalise(df, start_no)

    # Feed both preview and editor
    st.session_state["ledger_df"] = df.copy()
    st.session_state["editor_df"] = df.copy()

    st.subheader("Preview (first 15 rows)")
    st.dataframe(df.head(15), use_container_width=True)

    st.download_button(
        "Download ledger CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{Path(data_file.name).stem}_ledger.csv",
        mime="text/csv",
    )

# ──────────────────────────────────────────────────────────────────────────────
# Editable grid (in-memory). Optional loader to replace content.
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("---")
st.header("Editable grid")



if st.session_state["editor_df"] is None:
    st.info("Process a bank file above, or load a CSV/XLSX below.")
else:
    edited = st.data_editor(
        st.session_state["editor_df"],
        num_rows="dynamic",
        use_container_width=True,
        key="editor_grid",
    )
    st.session_state["editor_df"] = edited
    st.download_button(
        "Download edited CSV",
        data=edited.to_csv(index=False).encode("utf-8"),
        file_name="edited_ledger.csv",
        mime="text/csv",
    )

with st.expander("Load a different CSV/XLSX into the editor"):
    up2 = st.file_uploader("Choose CSV/XLSX", type=["csv", "xlsx", "xls"])
    if up2 is not None:
        if up2.name.lower().endswith((".xlsx", ".xls")):
            xls = pd.ExcelFile(up2)
            sheet_name = st.selectbox("Sheet", xls.sheet_names, index=0)
            df_in = pd.read_excel(xls, sheet_name=sheet_name, dtype=object)
        else:
            df_in = pd.read_csv(up2, dtype=object)
        st.session_state["editor_df"] = df_in
        st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# OAuth utilities
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class Token:
    access_token: str
    refresh_token: Optional[str]
    expires_at: float

def _save_token(tok: Dict):
    st.session_state["bexio_token"] = Token(
        access_token=tok["access_token"],
        refresh_token=tok.get("refresh_token"),
        expires_at=time.time() + int(tok.get("expires_in", 3600)) - 30,
    )

def _token_valid() -> bool:
    tok: Optional[Token] = st.session_state.get("bexio_token")
    return bool(tok and tok.access_token and time.time() < tok.expires_at)

def _refresh_token_if_needed():
    tok: Optional[Token] = st.session_state.get("bexio_token")
    if not tok or time.time() < tok.expires_at or not tok.refresh_token:
        return
    data = {"grant_type": "refresh_token", "refresh_token": tok.refresh_token}
    try:
        r = requests.post(TOKEN_URL, data=data, auth=(CLIENT_ID, CLIENT_SECRET), timeout=30)
    except Exception as e:
        st.warning(f"Token refresh failed: {e}")
        return
    if r.ok:
        _save_token(r.json())
    else:
        st.warning(f"Token refresh failed: {r.status_code} {r.text}")

def _auth_link(force_login: bool = False) -> str:
    from urllib.parse import urlencode
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": str(int(time.time())),
    }
    if force_login:
        params["prompt"] = "login"  # forces account/company re-pick
    return f"{AUTH_URL}?{urlencode(params)}"


def _is_authenticated() -> bool:
    return _token_valid()




def _exchange_code_for_token(code: str) -> bool:
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    try:
        r = requests.post(TOKEN_URL, data=data, timeout=30)
    except Exception as e:
        st.error(f"OAuth exchange failed (network): {e}")
        return False

    if r.ok:
        try:
            _save_token(r.json())
        except Exception:
            st.error(f"OAuth exchange failed: invalid JSON in token response: {r.text[:400]}")
            return False
        return True

    # Non-2xx: surface provider error payload if present
    try:
        err = r.json()
    except Exception:
        err = {"error": r.text[:400]}
    st.error(f"OAuth exchange failed: {r.status_code} {err}")
    return False


def _api_headers() -> Dict[str, str]:
    _refresh_token_if_needed()
    headers = {"Accept": "application/json"}
    tok: Optional[Token] = st.session_state.get("bexio_token")
    if tok and tok.access_token and time.time() < tok.expires_at:
        headers["Authorization"] = f"Bearer {tok.access_token}"
    return headers


    # OAuth fallback
    _refresh_token_if_needed()
    tok: Token = st.session_state.get("bexio_token")  # may be None
    if not tok:
        # No PAT and no OAuth token yet
        return {"Accept": "application/json"}
    return {"Accept": "application/json", "Authorization": f"Bearer {tok.access_token}"}

def get_userinfo() -> Optional[Dict]:
    try:
        headers = _api_headers()
        r = requests.get(USERINFO_URL, headers=headers, timeout=30)
        if r.ok:
            return r.json()
    except Exception as e:
        st.warning(f"userinfo failed: {e}")
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Connect to bexio UI
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("---")
st.header("Connect to bexio (auth.bexio.com)")


with st.expander("OAuth debug"):
    st.write({"issuer": ISSUER, "redirect_uri": REDIRECT_URI})
    st.code(_auth_link())  # verify redirect_uri

left, right = st.columns([1, 1])

with left:
    if not _token_valid():
        st.link_button("🔗 Connect to bexio (OAuth)", _auth_link())
    else:
        st.success("Connected via OAuth")
        info = get_userinfo() or {}
        email = info.get("email") or info.get("preferred_username")
        st.caption(f"Logged in as: {email or '—'}")
        st.link_button("Switch company (re-login)", _auth_link(force_login=True))
    st.caption(f"Issuer: {ISSUER}")
    st.caption(f"Redirect: {REDIRECT_URI}")

with right:
    qp = st.query_params
    code = qp.get("code")
    if isinstance(code, list):
        code = code[0]
    if code and not _token_valid():
        if _exchange_code_for_token(code):
            st.query_params.clear()
            st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# Posting to bexio – build payloads and send
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("---")
st.header("Post rows to bexio (manual entries)")

sources: Dict[str, pd.DataFrame] = {}
if isinstance(st.session_state.get("editor_df"), pd.DataFrame):
    sources["Edited grid"] = st.session_state["editor_df"]
if isinstance(st.session_state.get("ledger_df"), pd.DataFrame):
    sources["Processed ledger (original)"] = st.session_state["ledger_df"]

if not sources:
    st.info("Load/process data first to enable posting.")
else:
    choice = st.radio("Source", list(sources.keys()), horizontal=True)
    df_src = sources[choice]
    st.dataframe(df_src.head(10), width="stretch")  # use new API

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        dry_run = st.toggle("Dry run (don’t call API)", value=True, key="dry_run_toggle")
    with col2:
        row_limit = st.number_input("Max rows", min_value=1, value=50, step=10, key="row_limit_input")
    with col3:
        st.caption("Tip: start with Dry run to validate payloads.")

    # Button enabled if authenticated or you're in dry-run
    disabled = (not _is_authenticated()) and (not dry_run)
    if st.button("Post to bexio now", disabled=disabled):
        if not dry_run and not _is_authenticated():
            st.error("Please connect to bexio first (OAuth).")
            st.stop()

        n = min(len(df_src), int(row_limit))
        progress = st.progress(0.0)
        ok_count = 0
        errors = []

        for i, (_, row) in enumerate(df_src.head(n).iterrows(), start=1):
            payload = row_to_manual_entry(row)
            if dry_run:
                st.code(json.dumps(payload, ensure_ascii=False, indent=2))
                ok = True
                msg = "(dry-run)"
            else:
                ok, msg = post_manual_entry(payload)

            if ok:
                ok_count += 1
            else:
                errors.append({"row": i, "error": msg, "payload": payload})

            progress.progress(i / n)

        st.success(f"Done: {ok_count}/{n} successful.")
        if errors:
            with st.expander("Show errors"):
                for e in errors:
                    st.write(f"Row {e['row']}: {e['error']}")
                    st.code(json.dumps(e["payload"], ensure_ascii=False, indent=2))

def post_manual_entry(payload: Dict) -> Tuple[bool, str]:
    headers = _api_headers()  # OAuth only now
    if "Authorization" not in headers:
        return False, "Not authenticated (OAuth missing)"
    url = f"{API_BASE}{MANUAL_ENTRY_ENDPOINT}"
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.ok:
        return True, r.text
    return False, f"{r.status_code}: {r.text}"



# Choose source for posting
st.markdown("---")
st.header("Post rows to bexio (manual entries)")

sources: Dict[str, pd.DataFrame] = {}
if isinstance(st.session_state.get("editor_df"), pd.DataFrame):
    sources["Edited grid"] = st.session_state["editor_df"]
if isinstance(st.session_state.get("ledger_df"), pd.DataFrame):
    sources["Processed ledger (original)"] = st.session_state["ledger_df"]

if not sources:
    st.info("Load/process data first to enable posting.")
else:
    choice = st.radio("Source", list(sources.keys()), horizontal=True)
    df_src = sources[choice]
    st.dataframe(df_src.head(10), use_container_width=True)

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        dry_run = st.toggle("Dry run (don’t call API)", value=True)
    with col2:
        row_limit = st.number_input("Max rows", min_value=1, value=50, step=10)
    with col3:
        st.caption("Tip: start with Dry run to validate payloads.")

    if st.button("Post to bexio now", disabled=(not _token_valid()) and (not dry_run)):
        if not dry_run and not _token_valid():
            st.error("Please connect to bexio first.")
            st.stop()

        n = min(len(df_src), int(row_limit))
        progress = st.progress(0.0)
        ok_count = 0
        errors = []

        for i, (_, row) in enumerate(df_src.head(n).iterrows(), start=1):
            payload = row_to_manual_entry(row)
            if dry_run:
                st.code(json.dumps(payload, ensure_ascii=False, indent=2))
                ok = True
                msg = "(dry-run)"
            else:
                ok, msg = post_manual_entry(payload)

            if ok:
                ok_count += 1
            else:
                errors.append({"row": i, "error": msg, "payload": payload})

            progress.progress(i / n)

        st.success(f"Done: {ok_count}/{n} successful.")
        if errors:
            with st.expander("Show errors"):
                for e in errors:
                    st.write(f"Row {e['row']}: {e['error']}")
                    st.code(json.dumps(e["payload"], ensure_ascii=False, indent=
