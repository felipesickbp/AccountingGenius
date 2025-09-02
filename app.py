"""
Streamlit front-end – v3.4
——————————
* Adds an editable Excel/CSV grid (st.data_editor) with sheet picker
* Lets you save/download the edited file
* Adds "Connect to bexio" OAuth2 flow (idp.bexio.com)
* After login, you can post manual ledger entries to bexio via API
* Stores tokens in session and refreshes automatically (offline_access)

Notes
-----
- Fill in CLIENT_ID, CLIENT_SECRET, REDIRECT_URI (must match your app in bexio dev portal)
- Scopes may need adjusting depending on account: common picks include
  ['openid','profile','offline_access','contact_edit','kb_invoice_edit','bank_payment_edit']
  For journal/manual entries, consult docs and add relevant accounting scope.
- API base defaults to v2.0, change to v3.0 if your tenant uses it.
- Posting endpoint (MANUAL_ENTRY_ENDPOINT) may differ; verify in docs.

Security
--------
- Do NOT hardcode secrets in source for production. Use environment variables or a vault.
- Streamlit sharing deployments should use Secrets Manager.
"""
from __future__ import annotations

import io
import json
import os
import re
import time
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
import yaml

# Optional: import your existing modules (leave as-is if present)
try:
    from bookkeeping_app import (
        read_bank_csv,
        normalise_columns,
        clean_description,
        KontierungEngine,
    )
    import raiffeisen_transform
except Exception:
    # If not available in this environment, continue without them.
    read_bank_csv = normalise_columns = clean_description = KontierungEngine = None  # type: ignore
    raiffeisen_transform = None  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# OAuth / API config (migrated to auth.bexio.com + OIDC discovery)
# ──────────────────────────────────────────────────────────────────────────────
CLIENT_ID = os.getenv("BEXIO_CLIENT_ID", "bc3d501e-bbfb-4d6f-ae59-6f93629cbe0f")
CLIENT_SECRET = os.getenv("BEXIO_CLIENT_SECRET", "c6PO3_UaWnNHNkmm3f1dwO7QepW_h0FCW3ReepxN0OS4ojvWpaYWosbthiNYZuxw5w7W8zrMgEi0kO5Di6E3lQ")
REDIRECT_URI = os.getenv("BEXIO_REDIRECT_URI", "http://localhost:8501")  # must match app settings
SCOPES = os.getenv(
    "BEXIO_SCOPES",
    "openid profile offline_access contact_edit kb_invoice_edit bank_payment_edit",
)

# Prefer OpenID Connect discovery from the new issuer
OIDC_ISSUER = os.getenv("BEXIO_OIDC_ISSUER", "https://auth.bexio.com")
DISCOVERY_URL = f"{OIDC_ISSUER}/.well-known/openid-configuration"

@st.cache_data(ttl=3600, show_spinner=False)
def _discover_oidc() -> Dict[str, str]:
    try:
        r = requests.get(DISCOVERY_URL, timeout=30)
        if r.ok:
            return r.json()
    except Exception:
        pass
    # Fallback to sensible defaults on the new domain
    return {
        "authorization_endpoint": f"{OIDC_ISSUER}/authorize",
        "token_endpoint": f"{OIDC_ISSUER}/token",
        "userinfo_endpoint": f"{OIDC_ISSUER}/userinfo",
        "issuer": OIDC_ISSUER,
    }

_oidc = _discover_oidc()
AUTH_URL = _oidc.get("authorization_endpoint")
TOKEN_URL = _oidc.get("token_endpoint")
USERINFO_URL = _oidc.get("userinfo_endpoint")
ISSUER = _oidc.get("issuer", OIDC_ISSUER)

# API Base (v2.0 used widely; switch to 3.0 if needed)
API_BASE = os.getenv("BEXIO_API_BASE", "https://api.bexio.com/2.0")
MANUAL_ENTRY_ENDPOINT = os.getenv("BEXIO_MANUAL_ENTRY_ENDPOINT", "/accounting/manual_entries")

# ──────────────────────────────────────────────────────────────────────────────
# Streamlit page
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Bank ↦ Ledger (+ Excel editor + bexio)", layout="wide")
st.title("Bank Statement → Ledger CSV · Excel editor · bexio posting")

BANKS   = ["PostFinance", "Raiffeisen"]
CLIENTS = ["DB Financial", "Example AG", "Other Ltd"]

left, right = st.columns([1, 1])
with left:
    bank   = st.selectbox("Bank", BANKS, index=0)
with right:
    client = st.selectbox("Client (local profile)", CLIENTS, index=0)

start_no = st.number_input(
    "First voucher number (Belegnummer-Start)", min_value=1, value=1, step=1,
)

# Keyword→Konto YAML
cfg_path = Path("configs") / f"{client.lower().replace(' ', '_')}.yaml"
default_yaml = (
    cfg_path.read_text("utf-8") if cfg_path.exists() else "keywords:\n  \"coop|migros\": 4050\n"
)
yaml_text = st.text_area(
    "Keyword → Konto mapping (YAML)", value=default_yaml, height=160
)

file_types = {"PostFinance": ["csv"], "Raiffeisen": ["xlsx", "xls"]}
data_file  = st.file_uploader(
    f"Upload {bank} statement ({', '.join(file_types[bank])})",
    type=file_types[bank],
)

# ──────────────────────────────────────────────────────────────────────────────
# Template & VAT helpers (unchanged from v3.3, with small tweaks)
# ──────────────────────────────────────────────────────────────────────────────
TEMPLATE_ORDER = [
    "Belegnummer", "Datum", "Beschreibung", "Betrag", "Währung", "Wechselkurs",
    "Soll", "Haben", "MWST Code", "MWST Konto",
]

_MWST_ACCOUNTS = {
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

    # Belegnummer
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
    mask = df["Soll"].isin(_MWST_ACCOUNTS) | df["Haben"].isin(_MWST_ACCOUNTS)
    df.loc[mask, "MWST Code"]  = df.loc[mask, "MWST Code"].replace("", "VB81")
    df.loc[mask, "MWST Konto"] = (
        df.loc[mask, ["Soll", "Haben"]].bfill(axis=1).iloc[:, 0]
    )

    # Canonical order
    for col in TEMPLATE_ORDER:
        if col not in df.columns:
            df[col] = ""
    return df[TEMPLATE_ORDER]


# ──────────────────────────────────────────────────────────────────────────────
# Import + Preview button (existing flow)
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
        df["amount"] = (
            df["amount"].astype(str).str.replace("'", "").str.replace(",", ".").astype(float)
        )
        df["date"] = pd.to_datetime(df["date"], dayfirst=True).dt.strftime("%d.%m.%Y")
        if engine:
            df["account"] = df["description"].apply(engine.classify).astype(str)
        else:
            df["account"] = ""
        df["soll"]  = df.apply(lambda r: "1020" if r.amount > 0 else (r.account or ""), axis=1)
        df["haben"] = df.apply(lambda r: "1020" if r.amount < 0 else (r.account or ""), axis=1)
        df = df[["date", "description", "amount", "soll", "haben"]]
    else:
        # Raiffeisen: expect helper to give already-shaped df
        if raiffeisen_transform is None:
            st.error("The 'raiffeisen_transform' module is not available in this environment.")
            st.stop()
        try:
            df = raiffeisen_transform.process_excel(data_file, engine, start_no=start_no)
        except Exception as exc:
            st.error(f"❌ Failed to parse Excel: {exc}")
            st.stop()

    df = finalise(df, start_no)
    st.session_state["ledger_df"] = df.copy()

    st.subheader("Preview (first 15 rows)")
    st.dataframe(df.head(15), use_container_width=True)

    st.download_button(
        "Download ledger CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{Path(data_file.name).stem}_ledger.csv",
        mime="text/csv",
    )

# ──────────────────────────────────────────────────────────────────────────────
# NEW: Excel / CSV editor – upload, edit, save
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("---")
st.header("Excel / CSV editor (editable grid)")

up2 = st.file_uploader("Upload an Excel/CSV to edit", type=["xlsx", "xls", "csv"], key="editor")
if up2 is not None:
    edited_df: Optional[pd.DataFrame] = None
    sheet_name = None

    if up2.name.lower().endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(up2)
        sheet_name = st.selectbox("Sheet", xls.sheet_names, index=0)
        df_in = pd.read_excel(xls, sheet_name=sheet_name, dtype=object)
    else:
        df_in = pd.read_csv(up2, dtype=object)

    st.caption("Double-click cells to edit. You can add/remove rows via the + / trash icons.")
    edited_df = st.data_editor(
        df_in,
        num_rows="dynamic",
        use_container_width=True,
        key=f"grid_{sheet_name or 'csv'}",
    )

    colA, colB = st.columns(2)
    with colA:
        if st.button("Save edited file ↧"):
            buffer = io.BytesIO()
            if sheet_name:
                with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                    edited_df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                edited_df.to_csv(buffer, index=False)
            buffer.seek(0)
            mime = (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                if sheet_name else "text/csv"
            )
            fname = (
                f"edited_{Path(up2.name).stem}.xlsx" if sheet_name else f"edited_{Path(up2.name).name}"
            )
            st.download_button("Download edited file", data=buffer, file_name=fname, mime=mime)

    with colB:
        st.info("Below you can connect to bexio and post the edited ledger rows as manual entries.")

# ──────────────────────────────────────────────────────────────────────────────
# OAuth utilities (simple, session-based)
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
    if not tok:
        return
    if time.time() < tok.expires_at:
        return
    if not tok.refresh_token:
        return
    data = {
        "grant_type": "refresh_token",
        "refresh_token": tok.refresh_token,
    }
    try:
        r = requests.post(TOKEN_URL, data=data, auth=(CLIENT_ID, CLIENT_SECRET), timeout=30)
    except Exception as e:
        st.warning(f"Token refresh failed: {e}")
        return
    if r.ok:
        _save_token(r.json())
    else:
        st.warning(f"Token refresh failed: {r.status_code} {r.text}")


def _auth_link() -> str:
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": str(int(time.time())),
    }
    from urllib.parse import urlencode
    return f"{AUTH_URL}?{urlencode(params)}"


def _exchange_code_for_token(code: str) -> bool:
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
    }
    # Use HTTP Basic auth for client credentials (recommended by many OIDC providers)
    try:
        r = requests.post(TOKEN_URL, data=data, auth=(CLIENT_ID, CLIENT_SECRET), timeout=30)
    except Exception as e:
        st.error(f"OAuth exchange failed: {e}")
        return False
    if r.ok:
        _save_token(r.json())
        return True
    # Helpful diagnostics for common errors
    try:
        err = r.json()
    except Exception:
        err = {"error": r.text}
    st.error(f"OAuth exchange failed: {r.status_code} {err}")
    return False


def _api_headers() -> Dict[str, str]:
    _refresh_token_if_needed()
    tok: Token = st.session_state["bexio_token"]
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

cols = st.columns([1, 1, 2])
with cols[0]:
    if not _token_valid():
        st.link_button("🔗 Connect to bexio", _auth_link())
st.caption(f"Issuer: {ISSUER}")
    else:
        st.success("Connected to bexio")
        info = get_userinfo() or {}
        email = info.get("email") or info.get("preferred_username")
        st.caption(f"Logged in as: {email or '—'}")

with cols[1]:
    code = st.experimental_get_query_params().get("code", [None])[0]
    if code and not _token_valid():
        if _exchange_code_for_token(code):
            st.experimental_set_query_params()  # cleanup URL
            st.experimental_rerun()

with cols[2]:
    st.write(
        "If you manage multiple companies/mandates, authorize this app for each company separately.\n"
        "The access token returned by bexio is tied to the company you authorize."
    )

# ──────────────────────────────────────────────────────────────────────────────
# Posting to bexio – map rows → manual entry payloads
# ──────────────────────────────────────────────────────────────────────────────
def row_to_manual_entry(row: pd.Series) -> Dict:
    """Map our template row to a bexio manual entry payload.
    Adjust field names according to your tenant's endpoint spec.
    """
    # Parse date (expect dd.mm.yyyy)
    date_str = str(row.get("Datum", "")).strip()
    try:
        dt_iso = datetime.strptime(date_str, "%d.%m.%Y").date().isoformat() if date_str else None
    except Exception:
        dt_iso = None

    amount = row.get("Betrag", "")
    try:
        amount_f = float(str(amount).replace("'", "").replace(",", ".")) if amount != "" else 0.0
    except Exception:
        amount_f = 0.0

    payload = {
        # Common journal fields – adapt to real API contract
        "date": dt_iso,
        "text": str(row.get("Beschreibung", "")).strip() or None,
        "currency_code": (str(row.get("Währung", "CHF")).strip() or "CHF"),
        "exchange_rate": (str(row.get("Wechselkurs", "")).strip() or None),
        "lines": [
            {"account_id": str(row.get("Soll", "")).strip(),   "debit": round(abs(amount_f), 2),  "credit": 0.0},
            {"account_id": str(row.get("Haben", "")).strip(),  "debit": 0.0,                      "credit": round(abs(amount_f), 2)},
        ],
        # Optional VAT info (verify structure in docs)
        "vat_code": (str(row.get("MWST Code", "")).strip() or None),
        "vat_account": (str(row.get("MWST Konto", "")).strip() or None),
        # Your local voucher number
        "external_reference": str(row.get("Belegnummer", "")).strip() or None,
    }
    # Clean Nones / empties that might fail validation
    def _clean(d: Dict) -> Dict:
        return {k: v for k, v in d.items() if v not in (None, "")} 
    payload = _clean(payload)
    payload["lines"] = [ _clean(x) for x in payload.get("lines", []) ]
    return payload


def post_manual_entry(payload: Dict) -> Tuple[bool, str]:
    if not _token_valid():
        return False, "Not connected to bexio"
    headers = _api_headers()
    url = f"{API_BASE}{MANUAL_ENTRY_ENDPOINT}"
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.ok:
        return True, r.text
    return False, f"{r.status_code}: {r.text}"

# UI – choose a dataframe to post (edited grid or processed ledger)
post_source = None
if "grid_csv" in st.session_state:
    # from data_editor; resolved by key used above
    pass

postable_tabs = st.tabs(["Use edited grid", "Use processed ledger (above)"])
with postable_tabs[0]:
    grid_df = None
    if up2 is not None:
        # Retrieve latest grid data by the dynamic key used when rendering the editor
        # Note: Streamlit stores edited data in session_state under the key we supplied.
        key = f"grid_{(sheet_name or 'csv') if up2 is not None else 'csv'}"
        grid_df = st.session_state.get(key)
        if isinstance(grid_df, pd.DataFrame):
            st.dataframe(grid_df.head(10), use_container_width=True)
    if grid_df is not None:
        post_source = ("grid", grid_df)

with postable_tabs[1]:
    ledger_df = st.session_state.get("ledger_df")
    if isinstance(ledger_df, pd.DataFrame):
        st.dataframe(ledger_df.head(10), use_container_width=True)
        if post_source is None:
            post_source = ("ledger", ledger_df)

st.markdown("---")
st.header("Post rows to bexio (manual entries)")

col1, col2, col3 = st.columns([1,1,2])
with col1:
    dry_run = st.toggle("Dry run (don’t call API)", value=True)
with col2:
    row_limit = st.number_input("Max rows", min_value=1, value=50, step=10)
with col3:
    st.caption("Tip: start with a small subset in Dry run to validate payloads.")

if post_source is None:
    st.info("Load an edited grid or process a bank file first.")
else:
    source_name, df_src = post_source
    if st.button("Post to bexio now", disabled=not _token_valid() and not dry_run):
        if not dry_run and not _token_valid():
            st.error("Please connect to bexio first.")
            st.stop()
        n = min(len(df_src), int(row_limit))
        progress = st.progress(0)
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

# ──────────────────────────────────────────────────────────────────────────────
# Footer / help
# ──────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
    **Implementation notes**  
    • The OAuth flow uses `idp.bexio.com`. After you create a bexio app, set its Redirect URI to your Streamlit app URL.  
    • Tokens are stored in `st.session_state` and refreshed with `offline_access`.  
    • The posting endpoint path can vary by API version. Check your tenant’s API docs and update `BEXIO_API_BASE` and `BEXIO_MANUAL_ENTRY_ENDPOINT`.  
    • For multi-mandate usage, authorize one company at a time; tokens in bexio are scoped to the selected company during auth.  
    """
)

