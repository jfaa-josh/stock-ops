import streamlit as st
from streamlit_autorefresh import st_autorefresh
from dataclasses import dataclass
from typing import Any, Literal, Tuple, Optional, List, Dict
import sys
import logging
import random
import string
import uuid
import datetime as dt
import os

from api_factory import ApiLike, make_api
from ui_backend import DeploymentService
from utils import summarize_schedules_for_ui, parse_times_csv

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)

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

# ============ App state ============
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "CRASHED"}

if "TEST_MODE" not in st.session_state:
    st.session_state.TEST_MODE = (os.getenv("TEST_MODE", "0") == "1")

TEST_MODE: bool = st.session_state.TEST_MODE

if "api" not in st.session_state:
    if TEST_MODE:
        st.session_state.api = make_api("controller_driver", True)
    else:
        st.session_state.api = make_api("controller_driver", False)

api: ApiLike = st.session_state.api

if "services" not in st.session_state:
    st.session_state.services = {
        "hist": DeploymentService(st.session_state.api, provider="EODHD"),
        "stream": DeploymentService(st.session_state.api, provider="EODHD"),
    }

svc_hist = st.session_state.services["hist"]
svc_stream = st.session_state.services["stream"]

if "deploy_configs" not in st.session_state:
    # each cfg: {row_id, ticker, exchange, interval, frequency, start, end, deployment_name, deployment_id, flow_run_id, flow_run_name, flow_state}
    st.session_state.deploy_configs = []

if "skip_refresh_once" not in st.session_state:
    st.session_state.skip_refresh_once = False

# Helper: remove row by stable id (avoid idx/key reuse issues)
def remove_row(row_id: str):
    st.session_state.deploy_configs = [c for c in st.session_state.deploy_configs if c["row_id"] != row_id]

@dataclass
class Section:
    ns: str              # e.g., "hist" or "stream"
    title: str           # expander title
    api: ApiLike
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

    def run_deployment(self, cfg) -> Tuple[str, str]:
        if self.mode == "hist":
            fr_id, fr_name = svc_hist.run_historical(cfg)

        elif self.mode == "stream":
            try: # Parse duration for the command payload; fall back safely if bad text
                duration_int = int(cfg.get("duration","").strip())
                if duration_int <= 0:
                    raise ValueError
            except Exception:
                st.error("Duration must be a positive integer.")
                raise ValueError("Duration must be a positive integer.")

            fr_id, fr_name = svc_hist.run_stream(cfg)

        st.success(f"Flow triggered: {fr_name}")
        return fr_id, fr_name

    def _render_schedule_editor(self, exchange: str) -> Tuple[bool, Optional[List[Dict[str, Any]]]]:
        """
        Returns (use_schedule: bool, schedules: list[dict] | None).
        When multiple times are provided, returns one schedule per time (no backend changes required).
        """
        use_schedule = st.checkbox(
            "Schedule",
            key=f"{self.ns}_use_schedule",
            help=(
                "Enable to attach a recurring schedule to this deployment.\n"
                "If left unchecked, no schedule is created (one-off/manual runs only)."
            ),
        )
        if not use_schedule:
            return (False, None)

        if self.mode == "hist":
            tz_key = svc_hist.get_exchange_tz(exchange)
        elif self.mode == "stream":
            tz_key = svc_stream.get_exchange_tz(exchange)
        st.caption(f"Scheduling in exchange timezone: {tz_key}")

        freq = st.selectbox(
            "Frequency (RRULE FREQ)",
            options=["DAILY", "WEEKLY", "MONTHLY", "HOURLY", "MINUTELY"],
            index=0,
            key=f"{self.ns}_sched_freq",
            help=(
                "How often to run:\n"
                "• DAILY: every [Interval] days\n"
                "• WEEKLY: every [Interval] weeks (use BYDAY below)\n"
                "• MONTHLY: every [Interval] months (use times below)\n"
                "• HOURLY: every [Interval] hours (uses BYMINUTE/BYSECOND)\n"
                "• MINUTELY: every [Interval] minutes (uses BYSECOND)"
            ),
        )

        interval = st.number_input(
            "Interval",
            min_value=1, value=1, step=1,
            key=f"{self.ns}_sched_interval",
            help=(
                "Step size for FREQ. Examples:\n"
                "• DAILY + 1 → every day\n"
                "• WEEKLY + 2 → every other week\n"
                "• HOURLY + 6 → every 6 hours"
            ),
        )

        # DTSTART baseline
        start_cols = st.columns(2)
        dtstart_date = start_cols[0].date_input(
            "Start date (DTSTART)",
            key=f"{self.ns}_sched_dtstart_date",
            help="First eligible date for the schedule (exchange local date).",
        )
        dtstart_time = start_cols[1].time_input(
            "Start time (DTSTART)",
            value=dt.time(9, 30),
            key=f"{self.ns}_sched_dtstart_time",
            help="Local wall time in the exchange timezone. DST is preserved by Prefect.",
        )

        # Optional UNTIL
        set_until = st.checkbox(
            "Set end (UNTIL)",
            key=f"{self.ns}_sched_set_until",
            help="Optionally cap the schedule. The last occurrence will be at or before this local date/time.",
        )
        until_dt = None
        if set_until:
            until_cols = st.columns(2)
            until_date = until_cols[0].date_input(
                "End date",
                key=f"{self.ns}_sched_until_date",
                help="Final calendar date in the exchange timezone.",
            )
            until_time = until_cols[1].time_input(
                "End time",
                value=dt.time(23, 59),
                key=f"{self.ns}_sched_until_time",
                help=(
                    "Final wall time in the exchange timezone.\n"
                    "Prefect serializes this as UTC internally; local semantics are preserved."
                ),
            )
            if until_date and until_time:
                until_dt = dt.datetime.combine(until_date, until_time)

        # Weekly BYDAY (only when relevant)
        byweekday = None
        if freq == "WEEKLY":
            byweekday = st.multiselect(
                "Days of week (BYDAY)",
                options=["MO", "TU", "WE", "TH", "FR", "SA", "SU"],
                default=["MO", "TU", "WE", "TH", "FR"],
                key=f"{self.ns}_sched_byday",
                help="Select one or more weekdays. Example: MO,WE,FR for Monday/Wednesday/Friday.",
            )

        schedules: List[Dict[str, Any]] = []
        with st.expander("Times"):
            st.caption(
                "Choose either a single time-of-day (below) or multiple times using CSV.\n"
                "For multiple times, this UI creates one schedule per time."
            )

            multi_allowed = freq in {"DAILY", "WEEKLY", "MONTHLY", "YEARLY"}
            use_multi = st.checkbox(
                "Use multiple times per day",
                value=False,
                key=f"{self.ns}_sched_use_multi",
                disabled=not multi_allowed,
                help=(
                    "Enable to enter several times (e.g., 09:30, 12:00, 15:45).\n"
                    "Not applicable for HOURLY/MINUTELY."
                ) if multi_allowed else "Not applicable for HOURLY/MINUTELY",
            )

            if use_multi and multi_allowed:
                times_csv = st.text_input(
                    "Times (HH:MM, comma-separated)",
                    value="15:00,18:00,20:00",
                    key=f"{self.ns}_sched_times_csv",
                    help=(
                        "24-hour HH:MM list. Examples:\n"
                        "• 09:30,12:00,15:45 → three runs per selected day(s)\n"
                        "• 16:00 → one run at 4pm\n"
                        "Tip: For Mon/Wed/Fri at 3pm,6pm,8pm: set FREQ=WEEKLY, BYDAY=MO,WE,FR, Times=15:00,18:00,20:00"
                    ),
                )
                try:
                    times = parse_times_csv(times_csv)
                except Exception as e:
                    st.error(str(e))
                    return (True, None)

                # Build one schedule per time (no backend signature change required)
                for t in times:
                    dtstart_local = dt.datetime.combine(dtstart_date, t)
                    if until_dt and until_dt <= dtstart_local: # Sanity check
                        st.error("ALL UNTIL must be after DTSTART.")
                        return (True, None)

                    try:
                        sched = self.api.build_schedule(
                            timezone=tz_key,
                            freq=freq,
                            dtstart_local=dtstart_local,
                            interval=int(interval),
                            byweekday=byweekday,
                            until_local=until_dt,
                            byhour=t.hour,
                            byminute=t.minute,
                            bysecond=0,
                        )
                    except Exception as e:
                        st.error(f"Schedule invalid for time {t}: {e}")
                        return (True, None)
                    schedules.append(sched)
            else:
                # Single time-of-day controls (with disabling according to FREQ semantics)
                disable_byhour = (freq in {"HOURLY", "MINUTELY"})
                disable_byminute = (freq == "MINUTELY")
                byhour = st.number_input(
                    "BYHOUR (0–23)",
                    min_value=0, max_value=23,
                    value=dtstart_time.hour,
                    key=f"{self.ns}_sched_byhour",
                    disabled=disable_byhour,
                    help="Hour of day in exchange timezone. Ignored for HOURLY/MINUTELY.",
                )
                byminute = st.number_input(
                    "BYMINUTE (0–59)",
                    min_value=0, max_value=59,
                    value=dtstart_time.minute,
                    key=f"{self.ns}_sched_byminute",
                    disabled=disable_byminute,
                    help="Minute of hour. Ignored for MINUTELY.",
                )
                bysecond = st.number_input(
                    "BYSECOND (0–59)",
                    min_value=0, max_value=59,
                    value=0,
                    key=f"{self.ns}_sched_bysecond",
                    help="Second of minute. Example: 0 for on-the-minute.",
                )

                if not dtstart_date or not dtstart_time:
                    st.error("Please choose a DTSTART date and time.")
                    return (True, None)

                dtstart_local = dt.datetime.combine(dtstart_date, dtstart_time)
                if until_dt and until_dt <= dtstart_local: # Sanity check
                    st.error("UNTIL must be after DTSTART.")
                    return (True, None)

                try:
                    sched = self.api.build_schedule(
                        timezone=tz_key,
                        freq=freq,
                        dtstart_local=dtstart_local,
                        interval=int(interval),
                        byweekday=byweekday,
                        until_local=until_dt,
                        byhour=int(byhour),
                        byminute=int(byminute),
                        bysecond=int(bysecond),
                    )
                except Exception as e:
                    st.error(f"Schedule invalid: {e}")
                    return (True, None)
                schedules.append(sched)

        if schedules:
            st.success(f"Schedule ready ({len(schedules)} rule{'s' if len(schedules)!=1 else ''}).")
        return (True, schedules if schedules else None)

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

                tz_key = svc_hist.get_exchange_tz(new_exchange)
                st.caption(f"Data window in exchange timezone: {tz_key}")

                startcol_date, startcol_time = st.columns(2)
                start_date = startcol_date.date_input(f"Start date", value=dt.date(2025, 7, 2), key=f"{self.ns}_new_start_date")
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

                use_sched, sched_payload = (self._render_schedule_editor(new_exchange) or (False, None))

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
                        if use_sched and sched_payload:
                            cfg["schedules"] = sched_payload
                            cfg["schedule_summary"] = summarize_schedules_for_ui(cfg["schedules"])

                        svc_hist.create_deployment(cfg)
                        cfgs = self.get_cfgs()
                        cfgs.append(cfg)
                        self.set_cfgs(cfgs)
                        st.success("Configuration added!")
            elif self.mode == "stream":
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

                use_sched, sched_payload = (self._render_schedule_editor(new_exchange) or (False, None))

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
                        if use_sched and sched_payload:
                            cfg["schedules"] = sched_payload
                            cfg["schedule_summary"] = summarize_schedules_for_ui(cfg["schedules"])

                        svc_stream.create_deployment(cfg)
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
                    state = resp.get("state") or {}
                    cfg["flow_state"] = state.get("type")  # e.g., RUNNING, COMPLETED
                    if resp.get("name"):
                        cfg["flow_run_name"] = resp["name"]
                except Exception as e:
                    st.warning(f"Failed to check status for run {cfg['flow_run_id']}: {e}")

            # deployment status
            dep_status = "NOT_READY"
            if cfg.get("deployment_id"):
                try:
                    dep_resp = self.api.check_deployment_status(cfg["deployment_id"])
                    s = dep_resp.get("status")
                    dep_status = s.get("status") if isinstance(s, dict) else (s or "NOT_READY")
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
                                resp = self.api.delete_deployment(cfg["deployment_id"])
                            except Exception as e:
                                logger.exception("Delete API failed for %s", cfg["deployment_id"])
                                st.error(f"Delete failed: {e}")
                            else:
                                deleted = bool(resp.get("deleted")) if isinstance(resp, dict) else False
                                if deleted:
                                    cfgs[:] = [c for c in cfgs if c["row_id"] != cfg["row_id"]]
                                    self.set_cfgs(cfgs)
                                    st.success(f"Deleted configuration: {cfg['deployment_name']}")
                                    self.mark_skip_refresh()
                                    st.rerun()
                                else:
                                    st.warning(f"Unexpected delete response: {resp!r}")

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
                    summary = cfg.get("schedule_summary") or summarize_schedules_for_ui(cfg.get("schedules") or [])
                    if summary:
                        st.markdown('<div class="small-label">Schedule</div>', unsafe_allow_html=True)
                        st.markdown(f'<div class="value-strong">{summary}</div>', unsafe_allow_html=True)

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
