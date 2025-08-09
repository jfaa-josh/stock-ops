# TO TEST LOCALLY RUN IN POWERSHELL: streamlit run local_workflows/temp_local_dev.py

import streamlit as st
import time
import logging
import random
import string
import uuid
import datetime as dt
from datetime import datetime, UTC   # on Python < 3.11: from datetime import datetime, timezone as UTC
import streamlit.components.v1 as components

# ─────────────────────────────────────────────────────────────
# Optional CSS to prevent any button text wrapping
st.markdown("""
<style>
button[data-baseweb="button"] {
  white-space: nowrap !important;
  min-width: 10rem;
  padding: 0.6em 1.2em !important;
}
.small-label { font-size: 0.9rem; color: #666; margin-bottom: -0.35rem; }
.value-strong { font-weight: 600; }
</style>
""", unsafe_allow_html=True)
# ─────────────────────────────────────────────────────────────

# ============ Dummy API with session-backed state ============
def _ensure_api_state():
    st.session_state.setdefault("_deployments", {})  # deployment_id -> dict
    st.session_state.setdefault("_flow_runs", {})    # flow_run_id   -> dict

# simple slug generator for flow run names (e.g., "judicious_shrimp")
_ADJS = [
    "brisk", "careful", "diligent", "eager", "fearless", "gentle", "humble",
    "judicious", "keen", "lively", "mindful", "noble", "orderly", "patient",
    "quick", "robust", "steadfast", "tactful", "upbeat", "vivid", "wise", "youthful", "zesty"
]
_NOUNS = [
    "otter", "shrimp", "falcon", "lynx", "badger", "heron", "salmon", "sparrow",
    "walrus", "beaver", "cricket", "beetle", "panther", "ibis", "iguana", "swift",
    "tern", "seal", "puffin", "ferret", "marmot", "stoat", "fox", "jay"
]
def _slug():
    return f"{random.choice(_ADJS)}_{random.choice(_NOUNS)}"

class DummyAPI:
    def register_deployment(self, name, schedules):
        _ensure_api_state()
        dep_id = str(uuid.uuid4())
        # simulate a NOT_READY → READY transition after a few seconds
        st.session_state["_deployments"][dep_id] = {
            "id": dep_id,
            "name": name,
            "schedules": schedules,
            "created_ts": time.time(),
            "status": "NOT_READY",   # will become READY after ~3s
        }
        return {"id": dep_id, "name": name}

    def check_deployment_status(self, deployment_id):
        _ensure_api_state()
        meta = st.session_state["_deployments"].get(deployment_id)
        if not meta:
            return {"status": "NOT_READY"}
        # promote to READY ~3s after creation
        if (time.time() - meta["created_ts"]) >= 3 and meta["status"] != "READY":
            meta["status"] = "READY"
            st.session_state["_deployments"][deployment_id] = meta
        return {"status": meta["status"]}

    def delete_deployment(self, deployment_id):
        _ensure_api_state()
        st.session_state["_deployments"].pop(deployment_id, None)
        # clean up any flow runs tied to this deployment
        to_del = [fr for fr, fm in st.session_state["_flow_runs"].items()
                  if fm.get("deployment_id") == deployment_id]
        for fr in to_del:
            st.session_state["_flow_runs"].pop(fr, None)
        return {"deleted": True}

    def run_deployed_flow(self, deployment_id, provider, command_type, command):
        _ensure_api_state()
        if deployment_id not in st.session_state["_deployments"]:
            raise ValueError("Deployment does not exist")
        flow_run_id = str(uuid.uuid4())
        name = _slug()
        st.session_state["_flow_runs"][flow_run_id] = {
            "id": flow_run_id,
            "name": name,
            "deployment_id": deployment_id,
            "command": command,
            "start_ts": time.time(),
            "state_type": "PENDING",
        }
        return {"name": name, "id": flow_run_id}

    def check_flow_run_status(self, flow_run_id):
        _ensure_api_state()
        meta = st.session_state["_flow_runs"].get(flow_run_id)
        if not meta:
            return {"state_name": None, "state_type": None, "name": None}

        # simple time-based state machine
        elapsed = time.time() - meta["start_ts"]
        if elapsed < 5:
            state = "PENDING"
        elif elapsed < 15:
            state = "RUNNING"
        else:
            state = "COMPLETED"

        meta["state_type"] = state
        st.session_state["_flow_runs"][flow_run_id] = meta

        start_iso = datetime.fromtimestamp(meta["start_ts"], UTC).isoformat().replace("+00:00", "Z")
        end_iso = None
        if state in {"COMPLETED", "FAILED", "CANCELLED", "CRASHED"}:
            end_iso = datetime.now(UTC).isoformat().replace("+00:00", "Z")

        return {
            "id": flow_run_id,
            "name": meta["name"],
            "state_name": state,
            "state_type": state,
            "start_time": start_iso,
            "end_time": end_iso,
        }

api = DummyAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============ App state ============
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "CRASHED"}

if "deploy_configs" not in st.session_state:
    # each cfg: {row_id, ticker, exchange, interval, frequency, start, end, deployment_name, deployment_id, flow_run_id, flow_run_name, flow_state}
    st.session_state.deploy_configs = []

if "skip_refresh_once" not in st.session_state:
    st.session_state.skip_refresh_once = False

# Helper: unique 2-char suffix for dep names
def get_unique_id():
    used = {cfg["deployment_name"].rsplit("_", 1)[-1] for cfg in st.session_state.deploy_configs}
    chars = string.ascii_uppercase + string.digits
    while True:
        uid = "".join(random.choices(chars, k=2))
        if uid not in used:
            return uid

# Helper: remove row by stable id (avoid idx/key reuse issues)
def remove_row(row_id: str):
    st.session_state.deploy_configs = [c for c in st.session_state.deploy_configs if c["row_id"] != row_id]

def create_deployment(cfg):
    if cfg["deployment_id"] is None:
        resp = api.register_deployment(cfg["deployment_name"], schedules=None)
        cfg["deployment_id"] = resp["id"]
    # return current readiness; auto-refresh will advance it
    return api.check_deployment_status(cfg["deployment_id"]).get("status") == "READY"

def run_deployment(cfg):
    response = api.run_deployed_flow(
        cfg["deployment_id"], provider="EODHD", command_type="fetch_historical",
        command={
            "ticker":  cfg["ticker"],
            "exchange": cfg["exchange"],
            "interval": cfg["interval"],
            "start":   cfg["start"],
            "end":     cfg["end"],
        }
    )
    st.success(f"Flow triggered: {response['name']}")
    return response["id"], response["name"]

# 2) UI to add a new deployment
with st.expander("➕ Add new historical‐fetch config"):
    new_ticker   = st.text_input("Ticker",   value="SPY", key="new_ticker")
    new_exchange = st.text_input("Exchange", value="US",  key="new_exchange")

    # Frequency & Interval (dependent)
    freq_options = ["Interday", "Intraday"]
    new_frequency = st.selectbox("Frequency", options=freq_options, index=1, key="new_frequency")

    if new_frequency == "Interday":
        interval_options = ["d", "w", "m"]
        times_disabled = True
    else:
        interval_options = ["1m", "5m", "1h"]
        times_disabled = False

    new_interval = st.selectbox("Interval", options=interval_options, index=0, key="new_interval")

    # Date/time pickers with conditional disabling for Interday
    startcol_date, startcol_time = st.columns(2)
    start_date = startcol_date.date_input("Start date", value=dt.date(2025, 7, 2), key="new_start_date")
    start_time = startcol_time.time_input("Start time", value=dt.time(9, 30), key="new_start_time", disabled=times_disabled)

    endcol_date, endcol_time = st.columns(2)
    end_date = endcol_date.date_input("End date", value=dt.date(2025, 7, 2), key="new_end_date")
    end_time = endcol_time.time_input("End time", value=dt.time(9, 30), key="new_end_time", disabled=times_disabled)

    # Build start/end strings safely
    new_start = new_end = None
    if start_date and end_date:
        if new_frequency == "Interday":
            new_start = f"{start_date.strftime('%Y-%m-%d')}"
            new_end   = f"{end_date.strftime('%Y-%m-%d')}"
        else:
            if start_time and end_time:
                new_start = f"{start_date.strftime('%Y-%m-%d')} {start_time.strftime('%H:%M')}"
                new_end   = f"{end_date.strftime('%Y-%m-%d')} {end_time.strftime('%H:%M')}"

    if st.button("Add configuration"):
        if not all([new_ticker.strip(), new_exchange.strip(), new_interval.strip(), new_start, new_end]):
            st.error("Please fill in ticker, exchange, interval, start, and end.")
        else:
            unique_id = get_unique_id()
            cfg = {
                "row_id": str(uuid.uuid4()),        # stable per-row id for widget keys/deletion
                "ticker": new_ticker.strip(),
                "exchange": new_exchange.strip(),
                "interval": new_interval.strip(),
                "frequency": new_frequency,
                "start": new_start,
                "end": new_end,
                "deployment_name": f"{new_ticker.strip()}_{unique_id}",
                "deployment_id": None,
                "flow_run_id": None,
                "flow_run_name": None,
                "flow_state": None,
            }
            create_deployment(cfg)  # may be NOT_READY initially; auto-refresh will flip it
            st.session_state.deploy_configs.append(cfg)
            st.success("Configuration added!")

# 3) Render each config row
for cfg in list(st.session_state.deploy_configs):
    # Poll flow-run status once per render for active runs
    if cfg.get("flow_run_id") and (cfg.get("flow_state") not in TERMINAL):
        try:
            resp = api.check_flow_run_status(cfg["flow_run_id"])
            cfg["flow_state"] = resp.get("state_type")
            if resp.get("name"):
                cfg["flow_run_name"] = resp.get("name")
        except Exception as e:
            st.warning(f"Failed to check status for run {cfg['flow_run_id']}: {e}")

    # Precompute deployment status once (used for gating + display)
    dep_status = "NOT_READY"
    if cfg.get("deployment_id"):
        try:
            dep_status = api.check_deployment_status(cfg["deployment_id"]).get("status") or "NOT_READY"
        except Exception:
            pass

    with st.container():
        # Columns: Trigger | Delete | Dep Info | Run Info | Summary
        cols = st.columns([3, 3, 6, 6, 8], gap="small")

        # ---- Safe flags for gating ----
        flow_run_id = cfg.get("flow_run_id")
        flow_state  = cfg.get("flow_state")
        is_terminal    = (flow_state in TERMINAL) if (flow_state is not None) else False
        has_never_run  = not bool(flow_run_id)
        run_active     = bool(flow_run_id) and (not is_terminal)

        buttons_enabled = has_never_run or is_terminal
        can_trigger     = buttons_enabled and (dep_status == "READY")

        row_key = cfg["row_id"]  # stable key for widgets

        with cols[0]:
            if st.button(
                "Trigger",
                key=f"trigger_{row_key}",
                disabled=not can_trigger,
                help=None if can_trigger else ("Deployment not READY" if dep_status != "READY" else "Run in progress"),
            ):
                fr_id, fr_name = run_deployment(cfg)
                cfg["flow_run_id"]   = fr_id
                cfg["flow_run_name"] = fr_name
                resp = api.check_flow_run_status(fr_id)
                cfg["flow_state"] = resp.get("state_type")
                st.session_state.skip_refresh_once = True
                st.rerun()

        with cols[1]:
            if st.button(
                "Delete",
                key=f"del_{row_key}",
                disabled=run_active,
                help=None if not run_active else "Disabled while run is active",
            ):
                if cfg.get("deployment_id"):
                    try:
                        api.delete_deployment(cfg["deployment_id"])
                    except Exception as e:
                        st.warning(f"Delete API failed: {e}")
                remove_row(cfg["row_id"])
                st.success(f"Deleted configuration: {cfg['deployment_name']}")
                st.session_state.skip_refresh_once = True
                st.rerun()

        # ---- Info: Deployment side ----
        with cols[2]:
            st.markdown('<div class="small-label">Dep Name</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="value-strong">{cfg["deployment_name"]}</div>', unsafe_allow_html=True)

            st.markdown('<div class="small-label">Dep Status</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="value-strong">{dep_status}</div>', unsafe_allow_html=True)

        # ---- Info: Run side ----
        with cols[3]:
            st.markdown('<div class="small-label">Run Name</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="value-strong">{cfg.get("flow_run_name") or "—"}</div>', unsafe_allow_html=True)

            st.markdown('<div class="small-label">Run Status</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="value-strong">{cfg.get("flow_state") or "—"}</div>', unsafe_allow_html=True)

        # ---- Summary (col[4]) ----
        with cols[4]:
            st.markdown(
                f"{cfg['ticker']}.{cfg['exchange']} | {cfg['frequency']} | {cfg['interval']} | "
                f"{cfg['start']} → {cfg['end']}"
            )

    st.divider()

# 4) Auto-poll deployments and flow runs (pure Streamlit)
any_run_active = any(
    c.get("flow_run_id") and (c.get("flow_state") not in TERMINAL)
    for c in st.session_state.deploy_configs
)

any_dep_not_ready = False
for c in st.session_state.deploy_configs:
    if c.get("deployment_id"):
        try:
            s = api.check_deployment_status(c["deployment_id"]).get("status")
            if s != "READY":
                any_dep_not_ready = True
                break
        except Exception:
            pass

needs_refresh = any_run_active or any_dep_not_ready

# If we just clicked a button and there's something to monitor, kick the loop immediately (no wait)
if st.session_state.get("skip_refresh_once", False):
    st.session_state.skip_refresh_once = False
    if needs_refresh:
        st.rerun()

# Normal cadence polling: short sleep keeps UI responsive and still advances state
if needs_refresh:
    time.sleep(1.0)   # 1s = snappy; bump to 2–3s if you prefer less CPU churn
    st.rerun()
