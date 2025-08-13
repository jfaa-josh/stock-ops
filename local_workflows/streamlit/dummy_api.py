import time
import json
from datetime import datetime, UTC, date, time as dtime
from typing import Iterable, Optional, Any
import random
import logging
import streamlit as st
import uuid
from zoneinfo import ZoneInfo

from stockops.config import utils as cfg_utils # Add additional providers to config utils as needed

STRICT_TYPECHECK = False  # set True to raise on failures FOR TESTING ONLY

def _typename(x):
    return type(x).__name__

# ============ Dummy API with session-backed state ============
def _ensure_api_state():
    st.session_state.setdefault("_deployments", {})  # deployment_id -> dict
    st.session_state.setdefault("_flow_runs", {})    # flow_run_id   -> dict

logger = logging.getLogger(__name__)

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

    def get_tz(self, provider: str, exchange: str = "US") -> str:
        cfg = cfg_utils.ProviderConfig(provider, exchange)
        return cfg.tz_str

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

    def register_deployment(self, deployment_name: str,
                            schedules: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        _ensure_api_state()
        dep_id = str(uuid.uuid4())
        # simulate a NOT_READY → READY transition after a few seconds
        st.session_state["_deployments"][dep_id] = {
            "id": dep_id,
            "name": deployment_name,
            "schedules": schedules,
            "created_ts": time.time(),
            "status": "NOT_READY",   # will become READY after ~3s
        }

        try:
            if schedules:
                rrules = []
                for i, sch in enumerate(schedules, 1):
                    sched = sch.get("schedule") or {}
                    tz = sched.get("timezone", "UNKNOWN_TZ")
                    rr = sched.get("rrule", "").strip()
                    rrules.append(f"[{i}] timezone={tz}\n{rr}")
                logger.info(
                    "Registering deployment %r with %d schedule(s):\n%s",
                    deployment_name, len(schedules), "\n\n".join(rrules)
                )
                logger.debug(
                    "Full schedules payload JSON:\n%s",
                    json.dumps(schedules, indent=2, sort_keys=True)
                )
            else:
                logger.info("Registering deployment %r with no schedules", deployment_name)
        except Exception:
            logger.exception("Failed to log schedules")

        return {"id": dep_id, "name": deployment_name}

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

    def build_schedule(
        self,
        *,
        timezone: str,
        freq: str,                           # MINUTELY | HOURLY | DAILY | WEEKLY | MONTHLY | YEARLY
        dtstart_local: datetime,
        interval: int = 1,
        byweekday: Optional[Iterable[str]] = None,   # e.g. ["MO","TU","WE"]
        bymonth: Optional[Iterable[int]] = None,
        bymonthday: Optional[Iterable[int]] = None,
        bysetpos: Optional[Iterable[int]] = None,
        until_local: Optional[datetime | date] = None,
        byhour: Optional[int] = None,
        byminute: Optional[int] = None,
        bysecond: Optional[int] = None,
        active: bool = True,
    ) -> dict:
        valid_freq = {"MINUTELY", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "YEARLY"}
        if freq not in valid_freq:
            raise ValueError(f"Invalid FREQ: {freq}")
        if interval <= 0:
            raise ValueError("INTERVAL must be a positive integer")

        tz = ZoneInfo(timezone)
        if dtstart_local.tzinfo is None:
            dtstart_aware = dtstart_local.replace(tzinfo=tz)
        else:
            dtstart_aware = dtstart_local.astimezone(tz)

        # Default time parts from DTSTART
        h = dtstart_aware.hour if byhour is None else int(byhour)
        m = dtstart_aware.minute if byminute is None else int(byminute)
        s = dtstart_aware.second if bysecond is None else int(bysecond)

        parts = [f"FREQ={freq}", f"INTERVAL={interval}"]

        if byweekday:
            wd = [w.strip().upper() for w in byweekday]
            allowed = {"MO", "TU", "WE", "TH", "FR", "SA", "SU"}
            if not set(wd).issubset(allowed):
                raise ValueError(f"Invalid BYDAY tokens: {byweekday}")
            parts.append(f"BYDAY={','.join(wd)}")

        def _join_ints(name: str, values: Optional[Iterable[int]], lo: int, hi: int):
            if values is None:
                return
            vals = list(values)
            for v in vals:
                if v < lo or v > hi:
                    raise ValueError(f"{name} value {v} out of range [{lo},{hi}]")
            parts.append(f"{name}=" + ",".join(str(v) for v in vals))

        _join_ints("BYMONTH", bymonth, 1, 12)
        _join_ints("BYMONTHDAY", bymonthday, -31, 31)
        _join_ints("BYSETPOS", bysetpos, -366, 366)

        # Emit BY* fields conditionally to avoid restricting HOURLY/MINUTELY too much
        if freq in {"DAILY", "WEEKLY", "MONTHLY", "YEARLY"}:
            parts.append(f"BYHOUR={h}")
            parts.append(f"BYMINUTE={m}")
            parts.append(f"BYSECOND={s}")
        elif freq == "HOURLY":
            parts.append(f"BYMINUTE={m}")
            parts.append(f"BYSECOND={s}")
        elif freq == "MINUTELY":
            parts.append(f"BYSECOND={s}")

        # UNTIL handling (RFC 5545): if datetime -> convert to UTC and suffix Z; if date -> end-of-day local then convert
        if until_local is not None:
            if isinstance(until_local, date) and not isinstance(until_local, datetime):
                until_dt = datetime.combine(until_local, dtime(23, 59, 59))
            else:
                until_dt = until_local  # type: ignore[assignment]

            if until_dt.tzinfo is None:
                until_localized = until_dt.replace(tzinfo=tz)
            else:
                until_localized = until_dt.astimezone(tz)

            until_utc = until_localized.astimezone(ZoneInfo("UTC"))
            parts.append("UNTIL=" + until_utc.strftime("%Y%m%dT%H%M%SZ"))

        rrule_value = ";".join(parts)

        result: dict[str, Any] = {
            "active": bool(active),
            "schedule": {
                "rrule": rrule_value,
                "timezone": timezone,
                "dtstart": dtstart_aware.replace(microsecond=0).isoformat(),  # "2025-08-12T09:30:00-04:00"
            },
        }

        return result
