import streamlit as st
from streamlit_autorefresh import st_autorefresh
from dataclasses import dataclass, field
from typing import Any, Literal, Tuple, Optional, List, Dict
import sys, string, uuid, os, random
import logging
import datetime as dt
from zoneinfo import ZoneInfo

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

for noisy in (
    "watchdog",
    "watchdog.observers.inotify_buffer",
    "urllib3",
    "PIL",
):
    logging.getLogger(noisy).setLevel(logging.WARNING)

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
        "hist": DeploymentService(st.session_state.api, provider="EODHD", mode = "hist"),
        "stream": DeploymentService(st.session_state.api, provider="EODHD", mode = "stream"),
    }

svc_hist = st.session_state.services["hist"]
svc_stream = st.session_state.services["stream"]

@dataclass
class Section:
    ns: str              # e.g., "hist" or "stream"
    title: str           # expander title
    api: ApiLike
    mode: Literal["hist","stream"] = "hist"
    provider: str = "EODHD"  # currently only EODHD is supported
    cfg_key: str = field(init=False)
    skip_key: str = field(init=False)

    # ---- namespaced session state accessors ----
    @property
    def svc(self) -> DeploymentService:
        # Route to the correct DeploymentService instance
        return svc_hist if self.mode == "hist" else svc_stream

    def __post_init__(self):
        self.cfg_key  = f"{self.ns}_deploy_configs"
        self.skip_key = f"{self.ns}_skip_refresh_once"

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

    def run_deployment(self, cfg) -> Tuple[str, str]:
        fr_id, fr_name = self.svc.trigger_flow(cfg)

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

        tz_key = self.svc.get_exchange_tz(exchange)
        st.caption(f"Scheduling in exchange timezone: {tz_key}")

        # --- default DTSTART = ceil(now to next 15-min slot) in exchange tz ---
        now_local = dt.datetime.now(ZoneInfo(tz_key)).replace(second=0, microsecond=0)
        add = (15 - (now_local.minute % 15)) % 15
        dtstart_default = now_local if add == 0 else (now_local + dt.timedelta(minutes=add))

        _default_date = dtstart_default.date()
        _default_time = dtstart_default.time()

        start_cols = st.columns(2)
        dtstart_date = start_cols[0].date_input(
            "Start date (DTSTART)",
            value=_default_date,
            key=f"{self.ns}_sched_dtstart_date",
            help="First eligible date for the schedule (exchange local date).",
        )
        dtstart_time = start_cols[1].time_input(
            "Start time (DTSTART)",
            value=_default_time,
            key=f"{self.ns}_sched_dtstart_time",
            help="Local wall time in the exchange timezone. DST is preserved by Prefect.",
        )

        freq = st.selectbox(
            "Frequency (RRULE FREQ)",
            options=["DAILY", "WEEKLY", "MONTHLY", "HOURLY", "MINUTELY", "YEARLY"],
            index=0,
            key=f"{self.ns}_sched_freq",
            help=(
                "How often to run:\n"
                "• DAILY: every [Interval] days (choose time-of-day below)\n"
                "• WEEKLY: every [Interval] weeks (choose BYDAY below + time-of-day)\n"
                "• MONTHLY: every [Interval] months — choose either a day-of-month or an Nth weekday pattern, then time-of-day\n"
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
                "• MONTHLY + 3 → every 3 months\n"
                "• HOURLY + 6 → every 6 hours"
            ),
        )

        # WEEKLY BYDAY (only when relevant)
        byweekday = None
        if freq == "WEEKLY":
            byweekday = st.multiselect(
                "Days of week (BYDAY)",
                options=["MO", "TU", "WE", "TH", "FR", "SA", "SU"],
                default=["MO", "TU", "WE", "TH", "FR"],
                key=f"{self.ns}_sched_byday",
                help="Select one or more weekdays. Example: MO,WE,FR for Monday/Wednesday/Friday.",
            )

        # MONTHLY pattern switches
        bymonthday: Optional[List[int]] = None
        monthly_byweekday: Optional[List[str]] = None
        bysetpos: Optional[List[int]] = None

        if freq == "MONTHLY":
            mode_monthly = st.radio(
                "Monthly pattern",
                options=["Day of month", "Nth weekday"],
                horizontal=True,
                key=f"{self.ns}_sched_monthly_mode",
                help=(
                    "Choose 'Day of month' for a specific date (e.g., 15th). "
                    "Choose 'Nth weekday' for patterns like 1st Monday, 3rd Friday, or Last Friday."
                ),
            )

            if mode_monthly == "Day of month":
                day_choice = st.selectbox(
                    "Day of month (BYMONTHDAY)",
                    options=["1","2","3","4","5","6","7","8","9","10",
                            "11","12","13","14","15","16","17","18","19","20",
                            "21","22","23","24","25","26","27","28","29","30","31","Last day"],
                    index=14,  # 15th by default
                    key=f"{self.ns}_sched_bymonthday",
                    help="Pick a calendar day. 'Last day' maps to -1.",
                )
                bymonthday = [-1] if day_choice == "Last day" else [int(day_choice)]
                monthly_byweekday = None
                bysetpos = None
            else:
                ordinal = st.selectbox(
                    "Ordinal (BYSETPOS)",
                    options=["1st","2nd","3rd","4th","5th","Last"],
                    index=0,
                    key=f"{self.ns}_sched_bysetpos",
                    help="Which occurrence within the month.",
                )
                weekday = st.selectbox(
                    "Weekday (BYDAY)",
                    options=["MO", "TU", "WE", "TH", "FR", "SA", "SU"],
                    index=0,
                    key=f"{self.ns}_sched_monthly_byday",
                    help="Weekday for the monthly pattern.",
                )
                ord_map = {"1st":1, "2nd":2, "3rd":3, "4th":4, "5th":5, "Last":-1}
                bysetpos = [ord_map[ordinal]]
                monthly_byweekday = [weekday]
                bymonthday = None

        # -----------------------
        # TIMES (single or multi)
        # -----------------------
        times: Optional[List[dt.time]] = None
        single_byhour = single_byminute = single_bysecond = None

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
                    times = parse_times_csv(times_csv)  # -> list[dt.time]
                except Exception as e:
                    st.error(str(e))
                    return (True, None)
            else:
                # Single time-of-day controls (with disabling according to FREQ semantics)
                disable_byhour = (freq in {"HOURLY", "MINUTELY"})
                disable_byminute = (freq == "MINUTELY")
                single_byhour = st.number_input(
                    "BYHOUR (0–23)",
                    min_value=0, max_value=23,
                    value=dtstart_time.hour,
                    key=f"{self.ns}_sched_byhour",
                    disabled=disable_byhour,
                    help="Hour of day in exchange timezone. Ignored for HOURLY/MINUTELY.",
                )
                single_byminute = st.number_input(
                    "BYMINUTE (0–59)",
                    min_value=0, max_value=59,
                    value=dtstart_time.minute,
                    key=f"{self.ns}_sched_byminute",
                    disabled=disable_byminute,
                    help="Minute of hour. Ignored for MINUTELY.",
                )
                single_bysecond = st.number_input(
                    "BYSECOND (0–59)",
                    min_value=0, max_value=59,
                    value=0,
                    key=f"{self.ns}_sched_bysecond",
                    help="Second of minute. Example: 0 for on-the-minute.",
                )

        # -------------
        # UNTIL (last)
        # -------------
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

        # ----------------------
        # Build the schedules
        # ----------------------
        schedules: List[Dict[str, Any]] = []

        if not dtstart_date or not dtstart_time:
            st.error("Please choose a DTSTART date and time.")
            return (True, None)

        # Anchor boundary
        dtstart_anchor = dt.datetime.combine(dtstart_date, dtstart_time)

        if until_dt and until_dt <= dtstart_anchor:
            st.error("UNTIL must be after DTSTART.")
            return (True, None)

        if times is not None:
            # MULTI
            for t in times:
                try:
                    sched = self.svc.build_schedule(
                        timezone=tz_key,
                        freq=freq,
                        dtstart_local=dtstart_anchor,
                        interval=int(interval),
                        byweekday=(monthly_byweekday if freq == "MONTHLY" and monthly_byweekday else byweekday),
                        bymonthday=bymonthday,
                        bysetpos=bysetpos,
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
            # SINGLE
            try:
                sched = self.svc.build_schedule(
                    timezone=tz_key,
                    freq=freq,
                    dtstart_local=dtstart_anchor,
                    interval=int(interval),
                    byweekday=(monthly_byweekday if freq == "MONTHLY" and monthly_byweekday else byweekday),
                    bymonthday=bymonthday,
                    bysetpos=bysetpos,
                    until_local=until_dt,
                    byhour=int(single_byhour) if single_byhour is not None else None,
                    byminute=int(single_byminute) if single_byminute is not None else None,
                    bysecond=int(single_bysecond) if single_bysecond is not None else None,
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

                tz_key = self.svc.get_exchange_tz(new_exchange)
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
                            cfg["schedule_active"] = None
                            cfg["schedule_msg"] = ""
                            cfg["schedule_mode"] = "PRIME"

                        self.svc.create_deployment(cfg)
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
                    "Duration (hours)",
                    value="1.0",
                    key=f"{self.ns}_duration_text",
                    help="Enter a decimal number of hours"
                )

                # Optional validation preview
                try:
                    duration_float = float(duration_text.strip())
                    valid_duration = duration_float > 0
                except Exception:
                    duration_float = None
                    valid_duration = False

                use_sched, sched_payload = (self._render_schedule_editor(new_exchange) or (False, None))

                if st.button("Add configuration", key=f"{self.ns}_add_cfg_btn"):
                    if not all([new_ticker.strip(), new_exchange.strip(), valid_duration, stream_type.strip()]):
                        st.error("Please fill in ticker, exchange, a positive float duration, and integer stream type.")
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
                            cfg["schedule_active"] = None
                            cfg["schedule_msg"] = ""
                            cfg["schedule_mode"] = "PRIME"

                        self.svc.create_deployment(cfg)
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
                    self.svc.refresh_flow_state(cfg)
                except Exception as e:
                    st.warning(f"Failed to check status for run {cfg['flow_run_id']}: {e}")

            # deployment status (+ derive schedule state from deployment response)
            dep_status = "NOT_READY"
            dep_id = cfg.get("deployment_id")
            if dep_id:
                dep_status = self.svc.try_refresh_deployment_status(cfg, throttle_s=0.0)  # force-refresh
                if dep_status == "DELETED" or cfg.get("deleted_on_server"):
                    cfgs[:] = [c for c in cfgs if c["row_id"] != cfg["row_id"]]
                    self.set_cfgs(cfgs)
                    st.info(f"Deployment removed on server; deleted '{cfg['deployment_name']}' from UI.")
                    self.mark_skip_refresh()
                    st.rerun()
                    return

            is_scheduled_row = bool(cfg.get("schedules"))
            with st.container():
                cols = st.columns([4, 3, 5, 8, 8], gap="small")

                flow_run_id = cfg.get("flow_run_id")
                flow_state  = cfg.get("flow_state")
                is_terminal   = (flow_state in TERMINAL) if (flow_state is not None) else False
                has_never_run = not bool(flow_run_id)
                run_active    = bool(flow_run_id) and (not is_terminal)

                buttons_enabled = has_never_run or is_terminal
                can_trigger     = (not run_active) and buttons_enabled and (dep_status == "READY")

                row_key = cfg["row_id"]

                with cols[0]:
                    if is_scheduled_row:
                        mode = cfg.get("schedule_mode", "PRIME")

                        # Decide label by mode
                        label = {
                            "PRIME":  "Schedule Runs",
                            "ACTIVE": "Pause Schedule",
                            "PAUSED": "Resume",
                        }.get(mode, "Schedule Runs")

                        # Disable logic:
                        if label == "Schedule Runs":
                            disabled = (dep_status != "READY")  # allow scheduling even if a run is active
                            help_txt = None if not disabled else "Deployment not READY"
                        else:
                            disabled = False
                            help_txt = None

                        if st.button(
                            label,
                            key=f"{self.ns}_sched_{row_key}",
                            disabled=disabled,
                            help=help_txt,
                        ):
                            dep_id = cfg.get("deployment_id")

                            # Handle each label’s action
                            if label == "Schedule Runs":
                                try:
                                    self.svc.schedule_deployment(cfg)

                                    self.svc.try_refresh_deployment_status(cfg, throttle_s=0.0)
                                    self.mark_skip_refresh()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to schedule: {e}")

                            elif label == "Pause Schedule":
                                # POST /api/deployments/{deployment_id}/pause_deployment
                                try:
                                    self.svc.pause_schedule(dep_id)

                                    self.svc.try_refresh_deployment_status(cfg, throttle_s=0.0)
                                    self.mark_skip_refresh()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to pause schedule: {e}")

                            else:  # "Resume"
                                # POST /api/deployments/{deployment_id}/resume_deployment
                                try:
                                    self.svc.resume_schedule(dep_id)

                                    self.svc.try_refresh_deployment_status(cfg, throttle_s=0.0)
                                    self.mark_skip_refresh()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to resume schedule: {e}")
                    else:
                        # ORIGINAL Trigger button, with the earlier minimal change you accepted:
                        # can_trigger = (not run_active) and buttons_enabled and (dep_status == "READY")
                        if st.button(
                            "Trigger",
                            key=f"{self.ns}_trigger_{row_key}",
                            disabled=not can_trigger,
                            help=None if can_trigger else ("Deployment not READY" if dep_status != "READY" else "Run in progress"),
                        ):
                            fr_id, fr_name = self.run_deployment(cfg)
                            cfg["flow_run_id"]   = fr_id
                            cfg["flow_run_name"] = fr_name
                            self.svc.refresh_flow_state(cfg)
                            self.mark_skip_refresh()

                with cols[1]:
                    if st.button(
                        "Delete",
                        key=f"{self.ns}_del_{row_key}",
                        disabled=run_active,
                        help=None if not run_active else "Disabled while run is active",
                    ):
                        dep_id = cfg.get("deployment_id")
                        if dep_id:
                            # DELETE /api/deployments/{deployment_id}
                            try:
                                self.svc.delete_active_deployment(dep_id)
                            except Exception as e:
                                logger.exception("Delete API failed for %s", cfg["deployment_id"])
                                st.error(f"Delete failed: {e}")
                                return
                        # Remove locally exactly once
                        cfgs[:] = [c for c in cfgs if c["row_id"] != cfg["row_id"]]
                        self.set_cfgs(cfgs)
                        st.success(f"Deleted configuration: {cfg['deployment_name']}")
                        self.mark_skip_refresh()
                        st.rerun()

                with cols[2]:
                    st.markdown('<div class="small-label">Dep Name</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="value-strong">{cfg["deployment_name"]}</div>', unsafe_allow_html=True)
                    st.markdown('<div class="small-label">Dep Status</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="value-strong">{dep_status}</div>', unsafe_allow_html=True)

                    if is_scheduled_row:
                        if cfg.get("schedule_msg"):
                            st.markdown('<div class="small-label">Schedule Status</div>', unsafe_allow_html=True)
                            st.markdown(f'<div class="value-strong">{cfg["schedule_msg"]}</div>', unsafe_allow_html=True)

                with cols[3]:
                    if is_scheduled_row:
                        st.markdown('<div class="small-label">Schedule</div>', unsafe_allow_html=True)
                        source = (cfg.get("server_schedules") or (cfg.get("schedules") or []))
                        summary = summarize_schedules_for_ui(source, show_dtstart=True)
                        st.markdown(f'<div class="value-strong">{summary}</div>', unsafe_allow_html=True)

                    else:
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
                            f"{cfg['ticker']}.{cfg['exchange']} | stream={cfg['stream_type']} | duration={cfg['duration']}hr"
                        )

            st.divider()

    # ---- UI #4: auto-poll (isolated to this section) ----
    def auto_poll(self):
        cfgs = self.get_cfgs()

        any_dep_deleted = False
        any_dep_not_ready = False

        # One throttled refresh per row; the service handles caching/404/schedule fields
        for c in cfgs:
            status = self.svc.try_refresh_deployment_status(c, throttle_s=3.0)
            if status == "DELETED":
                any_dep_deleted = True
            elif status != "READY":
                any_dep_not_ready = True

        any_run_active = any(
            c.get("flow_run_id") and (c.get("flow_state") not in TERMINAL)
            for c in cfgs
        )

        # If we saw deletions, drop them immediately and re-render
        if any_dep_deleted:
            self.set_cfgs([c for c in cfgs if not c.get("deleted_on_server")])
            self.mark_skip_refresh()
            st.rerun()
            return

        needs_refresh = any_run_active or any_dep_not_ready

        if self.pop_skip_refresh() and needs_refresh:
            st.rerun()
            return

        if needs_refresh:
            interval = 1000 if any_run_active else 3000
            st_autorefresh(interval=interval, key=f"{self.ns}_autopoll")

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
