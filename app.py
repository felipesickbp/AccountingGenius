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
HARDCODED_CLIENT_ID     = "fe40a7f4-af9a-439d-ba6e-7fc83887c748"
HARDCODED_CLIENT_SECRET = "AOEYq3mT-H9ol06ZTKsZfLtSS7wB5uYnOuz9LUwIsYGP4umiyDxJJDHm2blAR1tH3tSwFbuEnnE8N1zucwpO1rY"
HARDCODED_REDIRECT_URI  = "https://accountinggenius-mdcq7sh8scyxglc7vvwcuh.streamlit.app"  # matches bexio app exactly

# Resolved config (Secrets/env take precedence, else hardcoded)
CLIENT_ID     = _get("BEXIO_CLIENT_ID",     HARDCODED_CLIENT_ID)
CLIENT_SECRET = _get("BEXIO_CLIENT_SECRET", HARDCODED_CLIENT_SECRET)
REDIRECT_URI  = _get("BEXIO_REDIRECT_URI",  HARDCODED_REDIRECT_URI)


# add email + profile; keep offline_access + your existing read scope(s)
BASE_SCOPES = "openid email profile offline_access contact_show"



# Bexio REST base + endpoint for manual journal entries (v2)
API_BASE = "https://api.bexio.com/2.0"
MANUAL_ENTRY_ENDPOINT = "/accounting/manual_entries"


# Fail fast if empty or still placeholders
if any(x in (None, "", "MY_CLIENT_ID_HERE", "MY_SECRET_KEY_HERE") for x in (CLIENT_ID, CLIENT_SECRET)):
    st.error("Missing BEXIO_CLIENT_ID / BEXIO_CLIENT_SECRET. Fill the HARDCODED_* values or set Streamlit secrets.")
    st.stop()



# ──────────────────────────────────────────────────────────────────────────────
# Streamlit page + state
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Bank ↦ Ledger (+ Excel editor + bexio)", layout="wide")
st.title("Bank Statement → Ledger CSV · Excel editor · bexio posting")
qp = st.query_params
err = qp.get("error")
err_desc = qp.get("error_description")
if err:
    st.error(f"OAuth error: {err} — {err_desc or ''}")
    # optional: offer a “Retry with minimal scopes” button



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




def _json_safe(obj):
    """Recursively convert pandas/numpy/scalars to JSON-serializable Python types."""
    import math
    try:
        import numpy as np
        import pandas as pd
    except Exception:
        np = None
        pd = None

    # None / simple types
    if obj is None or isinstance(obj, (str, bool, int, float)):
        # guard against non-finite floats
        if isinstance(obj, float) and not math.isfinite(obj):
            return None
        return obj

    # pandas NA / numpy NaN
    try:
        if pd is not None and pd.isna(obj):
            return None
    except Exception:
        pass

    # numpy scalars
    if np is not None:
        if isinstance(obj, (getattr(np, "integer", ()),)):
            return int(obj)
        if isinstance(obj, (getattr(np, "floating", ()),)):
            f = float(obj)
            return f if math.isfinite(f) else None

    # datetime-like
    from datetime import date, datetime
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()

    # containers
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]

    # fallback
    return str(obj)


# ──────────────────────────────────────────────────────────────────────────────
# OAuth utilities
# ──────────────────────────────────────────────────────────────────────────────

# --- OIDC (bexio Keycloak realm) ---
OIDC_ISSUER = _get("BEXIO_OIDC_ISSUER", "https://auth.bexio.com/realms/bexio")
DISCOVERY_URL = f"{OIDC_ISSUER}/.well-known/openid-configuration"

@st.cache_data(ttl=3600, show_spinner=False)
def _discover_oidc() -> Dict[str, str]:
    try:
        r = requests.get(DISCOVERY_URL, timeout=15)
        if r.ok:
            return r.json()
    except Exception:
        pass
    # Fallback to known Keycloak paths
    return {
        "authorization_endpoint": f"{OIDC_ISSUER}/protocol/openid-connect/auth",
        "token_endpoint":         f"{OIDC_ISSUER}/protocol/openid-connect/token",
        "userinfo_endpoint":      f"{OIDC_ISSUER}/protocol/openid-connect/userinfo",
        "issuer":                 OIDC_ISSUER,
    }

_oidc        = _discover_oidc()
AUTH_URL     = _oidc["authorization_endpoint"]
TOKEN_URL    = _oidc["token_endpoint"]
USERINFO_URL = _oidc["userinfo_endpoint"]
ISSUER       = _oidc.get("issuer", OIDC_ISSUER)


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
    # NEW: keep id_token for claim fallback
    st.session_state["bexio_id_token"] = tok.get("id_token")


def _current_scopes() -> str:
    tok: Optional[Token] = st.session_state.get("bexio_token")
    if not tok or not tok.access_token:
        return ""
    parts = tok.access_token.split(".")
    if len(parts) != 3:
        return ""
    import base64, json
    def b64url_decode(s: str) -> bytes:
        s += "=" * (-len(s) % 4)
        return base64.urlsafe_b64decode(s.encode("utf-8"))
    try:
        payload = json.loads(b64url_decode(parts[1]).decode("utf-8"))
        # Keycloak typically puts a space-separated string in "scope"
        return payload.get("scope", "")
    except Exception:
        return ""

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

def _auth_link(force_login: bool = False, scopes: Optional[str] = None) -> str:
    from urllib.parse import urlencode
    scope_str = (scopes or BASE_SCOPES).strip()
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": scope_str,
        "state": str(int(time.time())),
        # optional but recommended if you want refresh tokens reliably:
        "access_type": "offline",
        "prompt": "consent" if "offline_access" in scope_str else None,
    }
    params = {k: v for k, v in params.items() if v is not None}
    base = AUTH_URL or f"{OIDC_ISSUER}/protocol/openid-connect/auth"
    return f"{base}?{urlencode(params)}"

def _is_authenticated() -> bool:
    return _token_valid()
  
KNOWN_MANUAL_ENDPOINTS = [
    "/accounting/manual_entries",         # what you tried (likely not public)
    "/accounting/journal_entries",        # sometimes used naming
    "/accounting/manual-entries",         # v3-ish naming in some stacks
]

def smoke_test():
    headers = _api_headers()
    # Use an endpoint that matches the scopes you requested
    url = f"{API_BASE}/contact?limit=1"
    r = requests.get(url, headers=headers, timeout=15)
    return r.status_code, r.text[:400]


def post_manual_entry(payload: dict) -> tuple[bool, str]:
    headers = _api_headers()
    if "Authorization" not in headers:
        return False, "Not authenticated"

    headers["Content-Type"] = "application/json"
    safe_payload = _json_safe(payload)

    # one-time smoke test to confirm token/base work
    try:
        smoke = requests.get(f"{API_BASE}/kb_invoice?limit=1", headers=headers, timeout=10)
        if smoke.status_code in (401, 403):
            return False, f"{smoke.status_code} on smoke test – token lacks API access"
    except Exception as e:
        return False, f"Smoke test failed: {e}"

    # try a few known paths exactly once
    for path in KNOWN_MANUAL_ENDPOINTS:
        url = f"{API_BASE}{path}"
        r = requests.post(url, headers=headers, json=safe_payload, timeout=30)
        if r.status_code == 404:
            # keep trying alternatives
            continue
        if r.ok:
            return True, r.text
        if r.status_code == 403:
            return False, "403 – your app/user lacks Accounting (edit) for this company"
        if r.status_code == 422:
            return False, f"422 validation: {r.text}"
        return False, f"{r.status_code} {r.reason}: {r.text[:800]}"

    # all tried endpoints were 404
    return False, ("404 Not Found for all known endpoints. This usually means the "
                   "Manual Journal Entry route isn’t enabled for public developer apps. "
                   "Ask bexio support to confirm availability or partner access for "
                   "manual journal posting, or use an alternative workflow.")


def _exchange_code_for_token(code: str) -> bool:
    base_form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,  # include even with Basic (Keycloak-safe)
    }
    try:
        # Try HTTP Basic first
        r = requests.post(TOKEN_URL, data=base_form, auth=(CLIENT_ID, CLIENT_SECRET), timeout=30)
        if not r.ok:
            # Fallback to client_secret_post (no Basic)
            form2 = dict(base_form)
            form2["client_secret"] = CLIENT_SECRET
            r = requests.post(TOKEN_URL, data=form2, timeout=30)
    except Exception as e:
        st.error(f"OAuth exchange failed (network): {e}")
        return False

    if r.ok:
        try:
            tok = r.json()
            # sanity check
            if not tok.get("access_token"):
                st.error(f"OAuth exchange failed: missing access_token in response: {tok}")
                return False
            _save_token(tok)
        except Exception:
            st.error(f"OAuth exchange failed: invalid JSON in token response: {r.text[:400]}")
            return False
        return True

    # show server error
    try:
        err = r.json()
    except Exception:
        err = {"error": r.text[:400]}
    st.error(f"OAuth exchange failed: {r.status_code} {err}")
    return False


def row_to_manual_entry(row: pd.Series) -> Dict:
    # Date
    date_str = str(row.get("Datum", "")).strip()
    try:
        date_iso = datetime.strptime(date_str, "%d.%m.%Y").date().isoformat() if date_str else None
    except Exception:
        date_iso = None

    # Amount (abs, 2 decimals) as plain float
    raw_amt = row.get("Betrag", "")
    try:
        amt = float(str(raw_amt).replace("'", "").replace(",", ".")) if raw_amt != "" else 0.0
    except Exception:
        amt = 0.0
    amt_abs = round(abs(float(amt)), 2)

    # Accounts as simple strings
    soll  = (str(row.get("Soll", "")).strip() or "")
    haben = (str(row.get("Haben", "")).strip() or "")

    # Optional fields
    currency = (str(row.get("Währung", "CHF")).strip() or "CHF")
    fx_raw   = str(row.get("Wechselkurs", "")).strip()
    try:
        exchange_rate = float(fx_raw) if fx_raw else None
    except Exception:
        exchange_rate = None
    desc = (str(row.get("Beschreibung", "")).strip() or "")
    ref  = (str(row.get("Belegnummer", "")).strip() or "")

    payload = {
        "date": date_iso,
        "text": desc or None,
        "currency_code": currency,
        "exchange_rate": exchange_rate,  # numeric or None
        "lines": [
            {"account_id": soll,  "debit": float(amt_abs), "credit": 0.0},
            {"account_id": haben, "debit": 0.0,            "credit": float(amt_abs)},
        ],
        "external_reference": ref or None,
        # "vat_code": (str(row.get("MWST Code", "")).strip() or None),
        # "vat_account": (str(row.get("MWST Konto", "")).strip() or None),
    }

    # remove empties, then sanitize for JSON
    def _clean(d: Dict) -> Dict:
        return {k: v for k, v in d.items() if v not in ("", None, [], {})}
    payload = _clean(payload)
    payload["lines"] = [_clean(l) for l in payload.get("lines", [])]

    return _json_safe(payload)


def _api_headers() -> Dict[str, str]:
    _refresh_token_if_needed()
    headers = {"Accept": "application/json"}
    tok: Optional[Token] = st.session_state.get("bexio_token")
    if tok and tok.access_token and time.time() < tok.expires_at:
        headers["Authorization"] = f"Bearer {tok.access_token}"
    return headers


def get_userinfo() -> Optional[Dict]:
    headers = _api_headers()

    # 1) Try the userinfo endpoint
    try:
        r = requests.get(USERINFO_URL, headers=headers, timeout=30)
        if r.ok:
            js = r.json()
            if js:
                return js
    except Exception as e:
        st.warning(f"userinfo failed: {e}")

    # 2) Fallback to id_token claims
    idt = st.session_state.get("bexio_id_token")
    if idt:
        try:
            import base64, json
            p = idt.split(".")[1]
            p += "=" * (-len(p) % 4)
            return json.loads(base64.urlsafe_b64decode(p.encode("utf-8")))
        except Exception:
            pass

    # 3) Last fallback: decode access token payload for 'sub'
    tok = st.session_state.get("bexio_token")
    if tok and tok.access_token and "." in tok.access_token:
        try:
            import base64, json
            parts = tok.access_token.split(".")
            p = parts[1] + "=" * (-len(parts[1]) % 4)
            return {"sub": json.loads(base64.urlsafe_b64decode(p.encode("utf-8"))).get("sub")}
        except Exception:
            pass

    return None

# ──────────────────────────────────────────────────────────────────────────────
# Connect to bexio UI
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("---")
st.header("Connect to bexio (auth.bexio.com)")

with st.expander("OAuth debug"):
    st.write({"issuer": ISSUER, "redirect_uri": REDIRECT_URI})

    preset = st.selectbox(
    "Scope preset",
    [
        "openid offline_access contact_show",
        "openid offline_access contact_show kb_invoice_show",
        "openid offline_access contact_show bank_account_show",
        "openid offline_access all",
    ],
    index=0,
)

    scopes_input = st.text_input(
        "Scopes to request (space-separated)",
        value=preset,
        help="Start with 'openid'. Add others one by one once login succeeds."
    )

    st.code(_auth_link(scopes=scopes_input))
with st.expander("OAuth debug"):
    st.write({"issuer": ISSUER, "redirect_uri": REDIRECT_URI})
    extra = st.text_input("Extra scopes (advanced)", value=EXTRA_SCOPES_DEFAULT)
    effective_scopes = (BASE_SCOPES + " " + extra).strip() if extra else BASE_SCOPES
    st.code(_auth_link(scopes=effective_scopes))


# keep your smoke_test() definition where it is, but do NOT call it at top level

left, right = st.columns([1, 1])
with left:
    tok_ok = _token_valid()  # safe to call now; helpers already defined below this line
    if not tok_ok:
        st.link_button("🔗 Connect to bexio (OAuth)", _auth_link(scopes=SCOPE_PRESET))
    else:
        st.success("Connected via OAuth")

        who = (
            info.get("email")
            or info.get("preferred_username")
            or info.get("name")
            or info.get("sub")  # opaque user id fallback
        )
        st.caption(f"Logged in as: {who or '—'}")

        sc = _current_scopes()
        st.caption(f"Token scopes: {sc or '(unavailable)'}")

        # 🔹 run smoke test here instead of at top level
        code, txt = smoke_test()
        st.caption(f"API smoke test: {code}")
        if code != 200:
            st.warning(
                "Token is valid for login but not for the API. This usually means your app/client "
                "is not yet allowed to call the API or the required API scope(s) are missing."
            )


    st.caption(f"Issuer: {ISSUER}")
    st.caption(f"Redirect: {REDIRECT_URI}")

with right:
    qp = st.query_params
    err = qp.get("error")
    err_desc = qp.get("error_description")
    if err:
        st.error(f"OIDC error: {err} – {err_desc}")
        # optional: clear the params so the page resets
        # st.query_params.clear()

    code = qp.get("code")
    if isinstance(code, list):
        code = code[0]
    if code and not _token_valid():
        if _exchange_code_for_token(code):
            st.query_params.clear()
            st.rerun()


with st.expander("Troubleshooting"):
    if st.button("Reset auth & clear caches"):
        st.session_state.pop("bexio_token", None)
        try:
            st.cache_data.clear()
        except Exception:
            pass
        st.success("Cleared. Click 'Connect to bexio' again.")

with st.expander("Diagnostics: OIDC endpoints"):
    try:
        r = requests.get(DISCOVERY_URL, timeout=10)
        st.write({"discovery_url": DISCOVERY_URL, "status": r.status_code, "ok": r.ok})
        if r.ok:
            disc = r.json()
            st.write({"scopes_supported": disc.get("scopes_supported", [])})
            st.write({"token_endpoint_auth_methods_supported": disc.get("token_endpoint_auth_methods_supported", [])})
    except Exception as e:
        st.write({"discovery_url": DISCOVERY_URL, "error": str(e)})

    st.write({"auth_url": _auth_link(scopes=scopes_input)})

with st.expander("Auth status"):
    tok = st.session_state.get("bexio_token")
    st.write({
        "has_token": bool(tok),
        "token_valid": _token_valid(),
        "expires_at": getattr(tok, "expires_at", None),
        "now": time.time(),
        "scopes(decoded)": _current_scopes() or "(n/a)",
    })
with st.expander("Token claims (debug)"):
    tok = st.session_state.get("bexio_token")
    if not tok:
        st.write("No token")
    else:
        parts = tok.access_token.split(".")
        if len(parts) == 3:
            import base64, json
            def b64url(s): s += "=" * (-len(s) % 4); return base64.urlsafe_b64decode(s.encode())
            try:
                header  = json.loads(b64url(parts[0]).decode())
                payload = json.loads(b64url(parts[1]).decode())
                st.json({
                    "iss": payload.get("iss"),
                    "aud": payload.get("aud"),
                    "azp": payload.get("azp"),
                    "scope": payload.get("scope"),
                    "resource_access": payload.get("resource_access"),
                    "realm_access": payload.get("realm_access"),
                    "exp": payload.get("exp"),
                    # sometimes a company/org id is exposed; show a few commonly-used keys
                    "company": payload.get("company") or payload.get("tenant") or payload.get("org") or payload.get("bexio_company_id"),
                })
            except Exception as e:
                st.write(f"JWT decode error: {e}")
        else:
            st.write("Token is not a JWT?")


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
