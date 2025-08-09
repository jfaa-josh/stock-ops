# TO TEST LOCALLY RUN IN POWERSHELL: streamlit run local_workflows/local_streamlit_dev.py

import streamlit as st
from streamlit_autorefresh import st_autorefresh
from dataclasses import dataclass
from typing import Literal
import time
import logging
import random
import string
import uuid
import datetime as dt
from datetime import datetime, UTC   # on Python < 3.11: from datetime import datetime, timezone as UTC

STRICT_TYPECHECK = False  # set True to raise on failures FOR TESTING ONLY

def _typename(x):
    return type(x).__name__

# ─────────────────────────────────────────────────────────────
# 1) Use the wide page layout (expands beyond the centered column)
st.set_page_config(layout="wide")

# 2) Expand the main content container to the viewport width
st.markdown("""
<style>
/* Streamlit >= 1.25 robust selectors */
[data-testid="stAppViewContainer"] .main .block-container {
    max-width: 100% !important;    /* let content span the full width */
    padding-left: 1rem !important; /* reduce side gutters */
    padding-right: 1rem !important;
}

/* Optional: make the sidebar a bit wider (tweak as desired) */
[data-testid="stSidebar"] {
    min-width: 20rem !important;
}
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
    _EXPECTED_SCHEMAS = {
        "fetch_historical": {
            "ticker": str,
            "exchange": str,
            "interval": str,
            "start": str,
            "end": str,
        },
        "stream_live": {
            "ticker": str,
            "exchange": str,
            "streamType": str,  # "trades" | "quotes"
            "duration": int,    # seconds
        },
    }

    def _validate_and_log_call(self, deployment_id, provider, command_type, command):
        # Top-level param checks
        top_checks = {
            "deployment_id": (deployment_id, str),
            "provider":      (provider, str),
            "command_type":  (command_type, str),
            "command":       (command, dict),
        }

        top_results = {}
        for name, (val, expected) in top_checks.items():
            ok = isinstance(val, expected)
            top_results[name] = {"ok": ok, "got": _typename(val), "expected": expected.__name__}

        # Command schema checks (if we have a schema for this command_type)
        cmd_schema = self._EXPECTED_SCHEMAS.get(command_type, {})
        field_results = {}
        for k, expected_t in cmd_schema.items():
            v = None if not isinstance(command, dict) else command.get(k, None)
            ok = isinstance(v, expected_t)
            field_results[k] = {"ok": ok, "got": _typename(v), "expected": expected_t.__name__}

        # Pretty log to terminal
        lines = []
        lines.append("─" * 72)
        lines.append("DummyAPI.run_deployed_flow called with:")
        lines.append(f"  deployment_id: {deployment_id!r} ({_typename(deployment_id)})")
        lines.append(f"  provider     : {provider!r} ({_typename(provider)})")
        lines.append(f"  command_type : {command_type!r} ({_typename(command_type)})")
        lines.append(f"  command dict : ({_typename(command)}) ->")
        if isinstance(command, dict):
            for ck, cv in command.items():
                lines.append(f"      - {ck}: {cv!r} ({_typename(cv)})")
        else:
            lines.append("      <not a dict>")

        lines.append("Top-level type checks:")
        for name, res in top_results.items():
            status = "PASS" if res["ok"] else "FAIL"
            lines.append(f"  [{status}] {name}: expected {res['expected']}, got {res['got']}")

        if cmd_schema:
            lines.append(f"Command schema checks for {command_type!r}:")
            for name, res in field_results.items():
                status = "PASS" if res["ok"] else "FAIL"
                lines.append(f"  [{status}] {name}: expected {res['expected']}, got {res['got']}")
        else:
            lines.append(f"(No schema registered for command_type={command_type!r}; skipping field checks)")

        lines.append("─" * 72)
        logger.info("\n".join(lines))

        # Decide pass/fail
        ok_top = all(res["ok"] for res in top_results.values())
        ok_cmd = True if not cmd_schema else all(res["ok"] for res in field_results.values())
        all_ok = ok_top and ok_cmd

        if STRICT_TYPECHECK and not all_ok:
            # Raise a helpful error with specifics
            problems = []
            problems += [f"{k}: expected {v['expected']}, got {v['got']}" for k, v in top_results.items() if not v["ok"]]
            problems += [f"{k}: expected {v['expected']}, got {v['got']}" for k, v in field_results.items() if not v["ok"]]
            raise TypeError("Type check failed for run_deployed_flow params:\n  - " + "\n  - ".join(problems))

        return all_ok

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

        # >>> NEW: log + runtime type checks
        self._validate_and_log_call(deployment_id, provider, command_type, command)
        # <<<

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
        cfg["deployment_id"], cfg["provider"], command_type="fetch_historical",
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

@dataclass
class Section:
    ns: str              # e.g., "hist" or "stream"
    title: str           # expander title
    api: DummyAPI
    mode: Literal["hist","stream"] = "hist"
    provider: str = "EODHD"  # currently only EODHD is supported

    # ---- namespaced session state accessors ----
    @property
    def cfg_key(self) -> str:
        return f"{self.ns}_deploy_configs"

    @property
    def skip_key(self) -> str:
        return f"{self.ns}_skip_refresh_once"

    def get_cfgs(self):
        return st.session_state.setdefault(self.cfg_key, [])

    def set_cfgs(self, cfgs):
        st.session_state[self.cfg_key] = cfgs

    def mark_skip_refresh(self):
        st.session_state[self.skip_key] = True

    def pop_skip_refresh(self) -> bool:
        val = st.session_state.get(self.skip_key, False)
        st.session_state[self.skip_key] = False
        return val

    # ---- helpers (namespaced) ----
    def unique_suffix(self) -> str:
        used = {
            c["deployment_name"].rsplit("_", 1)[-1]
            for c in self.get_cfgs()
            if "deployment_name" in c
        }
        chars = string.ascii_uppercase + string.digits
        while True:
            uid = "".join(random.choices(chars, k=2))
            if uid not in used:
                return uid

    def remove_row(self, row_id: str):
        self.set_cfgs([c for c in self.get_cfgs() if c["row_id"] != row_id])

    def create_deployment(self, cfg):
        if cfg["deployment_id"] is None:
            resp = self.api.register_deployment(cfg["deployment_name"], schedules=None)
            cfg["deployment_id"] = resp["id"]
        s = self.api.check_deployment_status(cfg["deployment_id"]).get("status")
        return s == "READY"

    def run_deployment(self, cfg):
        if self.mode == "hist":
            command_type = "fetch_historical"
            command = {
                "ticker":   cfg["ticker"],
                "exchange": cfg["exchange"],
                "interval": cfg["interval"],
                "start":    cfg["start"],
                "end":      cfg["end"],
            }
        else:
            command_type = "stream_live"
            # Parse duration for the command payload; fall back safely if bad text
            try:
                duration_int = int(cfg.get("duration", "0").strip())
            except Exception:
                duration_int = 0
            command = {
                "ticker":     cfg["ticker"],
                "exchange":   cfg["exchange"],
                "streamType": cfg["stream_type"],  # "trades" | "quotes"
                "duration":   duration_int,        # int in payload
            }

        response = self.api.run_deployed_flow(
            cfg["deployment_id"],
            cfg["provider"],
            command_type=command_type,
            command=command,
        )
        st.success(f"Flow triggered: {response['name']}")
        return response["id"], response["name"]

    # ---- UI #2: Add config (identical structure; keys are prefixed) ----
    def render_adder(self):
        with st.expander(self.title):
            new_ticker   = st.text_input("Ticker",   value="SPY", key=f"{self.ns}_new_ticker")
            new_exchange = st.text_input("Exchange", value="US",  key=f"{self.ns}_new_exchange")

            if self.mode == "hist":
                # Historical: frequency/interval + start/end
                freq_options = ["Interday", "Intraday"]
                new_frequency = st.selectbox("Frequency", options=freq_options, index=1, key=f"{self.ns}_new_frequency")

                if new_frequency == "Interday":
                    interval_options = ["d", "w", "m"]
                    times_disabled = True
                else:
                    interval_options = ["1m", "5m", "1h"]
                    times_disabled = False

                new_interval = st.selectbox("Interval", options=interval_options, index=0, key=f"{self.ns}_new_interval")

                startcol_date, startcol_time = st.columns(2)
                start_date = startcol_date.date_input("Start date", value=dt.date(2025, 7, 2), key=f"{self.ns}_new_start_date")
                start_time = startcol_time.time_input("Start time", value=dt.time(9, 30), key=f"{self.ns}_new_start_time", disabled=times_disabled)

                endcol_date, endcol_time = st.columns(2)
                end_date = endcol_date.date_input("End date", value=dt.date(2025, 7, 2), key=f"{self.ns}_new_end_date")
                end_time = endcol_time.time_input("End time", value=dt.time(9, 30), key=f"{self.ns}_new_end_time", disabled=times_disabled)

                new_start = new_end = None
                if start_date and end_date:
                    if new_frequency == "Interday":
                        new_start = f"{start_date.strftime('%Y-%m-%d')}"
                        new_end   = f"{end_date.strftime('%Y-%m-%d')}"
                    else:
                        if start_time and end_time:
                            new_start = f"{start_date.strftime('%Y-%m-%d')} {start_time.strftime('%H:%M')}"
                            new_end   = f"{end_date.strftime('%Y-%m-%d')} {end_time.strftime('%H:%M')}"

                if st.button("Add configuration", key=f"{self.ns}_add_cfg_btn"):
                    if not all([new_ticker.strip(), new_exchange.strip(), new_interval.strip(), new_start, new_end]):
                        st.error("Please fill in ticker, exchange, interval, start, and end.")
                    else:
                        unique_id = self.unique_suffix()
                        cfg = {
                            "row_id": str(uuid.uuid4()),
                            "provider": self.provider,
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
                        self.create_deployment(cfg)
                        cfgs = self.get_cfgs()
                        cfgs.append(cfg)
                        self.set_cfgs(cfgs)
                        st.success("Configuration added!")
            else:
                # Stream: stream_type + duration
                stream_type = st.selectbox(
                    "Stream type",
                    options=["trades", "quotes"],
                    index=0,
                    key=f"{self.ns}_stream_type",
                )
                duration_text = st.text_input(
                    "Duration (seconds)",
                    value="60",
                    key=f"{self.ns}_duration_text",
                    help="Enter an integer number of seconds"
                )

                # Optional validation preview
                try:
                    duration_int = int(duration_text.strip())
                    valid_duration = duration_int > 0
                except Exception:
                    duration_int = None
                    valid_duration = False

                if st.button("Add configuration", key=f"{self.ns}_add_cfg_btn"):
                    if not all([new_ticker.strip(), new_exchange.strip(), valid_duration, stream_type.strip()]):
                        st.error("Please fill in ticker, exchange, a positive integer duration, and stream type.")
                    else:
                        unique_id = self.unique_suffix()
                        cfg = {
                            "row_id": str(uuid.uuid4()),
                            "provider": self.provider,
                            "ticker": new_ticker.strip(),
                            "exchange": new_exchange.strip(),
                            "stream_type": stream_type.strip(),   # "trades" | "quotes"
                            "duration": duration_text.strip(),    # keep original text for UI
                            "deployment_name": f"{new_ticker.strip()}_{unique_id}",
                            "deployment_id": None,
                            "flow_run_id": None,
                            "flow_run_name": None,
                            "flow_state": None,
                        }
                        self.create_deployment(cfg)
                        cfgs = self.get_cfgs()
                        cfgs.append(cfg)
                        self.set_cfgs(cfgs)
                        st.success("Configuration added!")

    # ---- UI #3: Per-row containers (identical; namespaced keys) ----
    def render_rows(self):
    # Work with the live list, but iterate over a copy for safety
        cfgs = self.get_cfgs()
        for cfg in list(cfgs):
            if cfg.get("flow_run_id") and (cfg.get("flow_state") not in TERMINAL):
                try:
                    resp = self.api.check_flow_run_status(cfg["flow_run_id"])
                    cfg["flow_state"] = resp.get("state_type")
                    if resp.get("name"):
                        cfg["flow_run_name"] = resp.get("name")
                except Exception as e:
                    st.warning(f"Failed to check status for run {cfg['flow_run_id']}: {e}")

            # deployment status
            dep_status = "NOT_READY"
            if cfg.get("deployment_id"):
                try:
                    dep_status = self.api.check_deployment_status(cfg["deployment_id"]).get("status") or "NOT_READY"
                except Exception:
                    pass

            with st.container():
                cols = st.columns([3, 3, 6, 6, 8], gap="small")

                flow_run_id = cfg.get("flow_run_id")
                flow_state  = cfg.get("flow_state")
                is_terminal   = (flow_state in TERMINAL) if (flow_state is not None) else False
                has_never_run = not bool(flow_run_id)
                run_active    = bool(flow_run_id) and (not is_terminal)

                buttons_enabled = has_never_run or is_terminal
                can_trigger     = buttons_enabled and (dep_status == "READY")

                row_key = cfg["row_id"]

                with cols[0]:
                    if st.button(
                        "Trigger",
                        key=f"{self.ns}_trigger_{row_key}",
                        disabled=not can_trigger,
                        help=None if can_trigger else ("Deployment not READY" if dep_status != "READY" else "Run in progress"),
                    ):
                        fr_id, fr_name = self.run_deployment(cfg)
                        cfg["flow_run_id"]   = fr_id
                        cfg["flow_run_name"] = fr_name
                        resp = self.api.check_flow_run_status(fr_id)
                        cfg["flow_state"] = resp.get("state_type")
                        self.mark_skip_refresh()

                with cols[1]:
                    if st.button(
                        "Delete",
                        key=f"{self.ns}_del_{row_key}",
                        disabled=run_active,
                        help=None if not run_active else "Disabled while run is active",
                    ):
                        if cfg.get("deployment_id"):
                            try:
                                self.api.delete_deployment(cfg["deployment_id"])
                            except Exception as e:
                                st.warning(f"Delete API failed: {e}")

                        # Remove from the *live* list in-place and persist
                        cfgs[:] = [c for c in cfgs if c["row_id"] != cfg["row_id"]]
                        self.set_cfgs(cfgs)

                        st.success(f"Deleted configuration: {cfg['deployment_name']}")
                        self.mark_skip_refresh()
                        st.rerun()   # or st.stop()

                with cols[2]:
                    st.markdown('<div class="small-label">Dep Name</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="value-strong">{cfg["deployment_name"]}</div>', unsafe_allow_html=True)
                    st.markdown('<div class="small-label">Dep Status</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="value-strong">{dep_status}</div>', unsafe_allow_html=True)

                with cols[3]:
                    st.markdown('<div class="small-label">Run Name</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="value-strong">{cfg.get("flow_run_name") or "—"}</div>', unsafe_allow_html=True)
                    st.markdown('<div class="small-label">Run Status</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="value-strong">{cfg.get("flow_state") or "—"}</div>', unsafe_allow_html=True)

                with cols[4]:
                    if self.mode == "hist":
                        st.markdown(
                            f"{cfg['ticker']}.{cfg['exchange']} | {cfg['frequency']} | {cfg['interval']} | "
                            f"{cfg['start']} → {cfg['end']}"
                        )
                    else:
                        st.markdown(
                            f"{cfg['ticker']}.{cfg['exchange']} | stream={cfg['stream_type']} | duration={cfg['duration']}s"
                        )

            st.divider()

    # ---- UI #4: auto-poll (isolated to this section) ----
    def auto_poll(self):
        cfgs = self.get_cfgs()

        any_run_active = any(
            c.get("flow_run_id") and (c.get("flow_state") not in TERMINAL)
            for c in cfgs
        )

        any_dep_not_ready = any(
            (self.api.check_deployment_status(c["deployment_id"]).get("status") != "READY")
            for c in cfgs if c.get("deployment_id")
        )

        needs_refresh = any_run_active or any_dep_not_ready

        # Instant UI update right after a button click (no sleep)
        if self.pop_skip_refresh() and needs_refresh:
            st.rerun()

        # Non-blocking periodic refresh driven by the browser
        if needs_refresh:
            st_autorefresh(interval=1000, key=f"{self.ns}_autopoll")  # 1s

    def render(self):
        self.render_adder()
        self.render_rows()
        self.auto_poll()

# === LAYOUT: two identical capabilities side-by-side ===
left_col, right_col = st.columns([1, 1], gap="large")

with left_col:
    Section(ns="hist", title="➕ Add new EODHD historical‐fetch config", api=api, mode="hist", provider = "EODHD").render()

with right_col:
    Section(ns="stream", title="➕ Add new EODHD stream‐fetch config", api=api, mode="stream", provider = "EODHD").render()
