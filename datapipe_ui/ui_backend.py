from api_factory import ApiLike
from zoneinfo import ZoneInfo
from typing import Iterable, Optional, Any, Dict, Tuple
from datetime import datetime, date, time as dtime
import requests
import time

from stockops.config import utils as cfg_utils  # Add additional providers to config utils as needed
from utils import norm_dep_status_value, derive_schedule_state_from_deployment

class DeploymentService:
    def __init__(self, api: ApiLike, provider: str = "EODHD", mode: str = "hist"):
        self.api = api
        self.provider = provider
        self.mode = mode

    # ---------------- existing methods ----------------
    def create_deployment(self, cfg: dict[str, Any]) -> bool:
        if cfg.get("deployment_id") is None:
            resp = self.api.register_deployment(cfg["deployment_name"])
            cfg["deployment_id"] = resp["id"]
        dep = self.api.check_deployment_status(cfg["deployment_id"])
        s = dep.get("status")
        status_str = s.get("status") if isinstance(s, dict) else s
        return status_str == "READY"


    def build_command(self, cfg: dict[str, Any]) -> dict[str, Any]:
        if self.mode == "hist":
            return {
                "ticker": cfg["ticker"], "exchange": cfg["exchange"],
                "interval": cfg["interval"], "start": cfg["start"], "end": cfg["end"],
            }
        elif self.mode == "stream":
            return {
                "tickers": cfg["ticker"], "exchange": cfg["exchange"],
                "stream_type": cfg["stream_type"], "duration": int(cfg["duration"]),
            }
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")

    def get_command_type(self) -> str:
        if self.mode == "hist":
            return "fetch_historical"
        if self.mode == "stream":
            return "start_stream"
        raise ValueError(f"Unsupported mode: {self.mode!r}")

    def trigger_flow(self, cfg: dict[str, Any]) -> Tuple[str, str]:
        command = self.build_command(cfg)
        command_type = self.get_command_type()
        resp = self.api.run_deployed_flow(cfg["deployment_id"], self.provider, command_type, command)
        return resp["id"], resp["name"]

    def get_exchange_tz(self, exchange: str) -> str:
        try:
            return cfg_utils.ProviderConfig(self.provider, exchange).tz_str
        except Exception:
            return "UTC"

    @staticmethod
    def normalize_state_type(resp: dict[str, Any]) -> Optional[str]:
        """
        Accepts either:
          - POST /deployments/{id}/create_flow_run (has top-level 'state_type'), or
          - GET /flow_runs/{id} (has nested 'state': {'type': ...})
        Returns a normalized state type string (e.g., 'PENDING', 'RUNNING', 'COMPLETED') or None.
        """
        return resp.get("state_type") or ((resp.get("state") or {}).get("type"))

    def refresh_flow_state(self, cfg: dict[str, Any]) -> Optional[str]:
        """
        Query current flow run status and write back to cfg:
          - cfg['flow_state']
          - cfg['flow_run_name'] (if present)
        Returns the normalized state string or None.
        """
        fr_id = cfg.get("flow_run_id")
        if not fr_id:
            return None
        resp = self.api.check_flow_run_status(fr_id)
        state = self.normalize_state_type(resp)
        if state:
            cfg["flow_state"] = state
        if resp.get("name"):
            cfg["flow_run_name"] = resp["name"]
        return state

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

        # Localize/normalize DTSTART to the schedule timezone
        if dtstart_local.tzinfo is None:
            dtstart_aware = dtstart_local.replace(tzinfo=tz)
        else:
            dtstart_aware = dtstart_local.astimezone(tz)

        # Default time parts from DTSTART when not explicitly provided
        h = dtstart_aware.hour if byhour is None else int(byhour)
        m = dtstart_aware.minute if byminute is None else int(byminute)
        s = dtstart_aware.second if bysecond is None else int(bysecond)

        # Build RRULE components (without DTSTART)
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

        # Emit BY* fields conditionally to avoid over-restricting HOURLY/MINUTELY
        if freq in {"DAILY", "WEEKLY", "MONTHLY", "YEARLY"}:
            parts.append(f"BYHOUR={h}")
            parts.append(f"BYMINUTE={m}")
            parts.append(f"BYSECOND={s}")
        elif freq == "HOURLY":
            parts.append(f"BYMINUTE={m}")
            parts.append(f"BYSECOND={s}")
        elif freq == "MINUTELY":
            parts.append(f"BYSECOND={s}")

        # UNTIL (convert to UTC Z)
        if until_local is not None:
            if isinstance(until_local, date) and not isinstance(until_local, datetime):
                until_dt = datetime.combine(until_local, dtime(23, 59, 59))
            else:
                until_dt = until_local  # datetime

            # Localize to schedule tz
            if until_dt.tzinfo is None:
                until_localized = until_dt.replace(tzinfo=tz)
            else:
                until_localized = until_dt.astimezone(tz)

            # Sanity check in local tz semantics
            if until_localized <= dtstart_aware:
                raise ValueError("UNTIL must be after DTSTART in local exchange time")

            # Append as UTC with Z
            until_utc = until_localized.astimezone(ZoneInfo("UTC"))
            parts.append("UNTIL=" + until_utc.strftime("%Y%m%dT%H%M%SZ"))

        # Format the RRULE line
        rrule_only = ";".join(parts)

        # Format DTSTART as local wall time with TZID (preserves local-time semantics across DST)
        dtstart_line = f"DTSTART;TZID={timezone}:" + dtstart_aware.strftime("%Y%m%dT%H%M%S")

        # Final multi-line rrule payload
        rrule_value = dtstart_line + "\nRRULE:" + rrule_only

        result: Dict[str, Any] = {
            "active": bool(active),
            "schedule": {
                "rrule": rrule_value,
                "timezone": timezone,  # keep for clarity: Prefect stores/display tz semantics
                # Do NOT include an 'anchor_date' or separate 'dtstart' key
            },
        }
        return result

    def schedule_deployment(self, cfg: dict[str, Any]) -> Optional[bool]:
        dep_id = cfg["deployment_id"]
        sched_list = cfg.get("schedules") or []
        if isinstance(sched_list, dict):
            sched_list = [sched_list]

        command = self.build_command(cfg)
        command_type = self.get_command_type()

        payload = []
        for s in sched_list:
            if not isinstance(s, dict) or "schedule" not in s:
                raise ValueError("Invalid schedule object; expected {'active': bool, 'schedule': {...}}")
            payload.append({
                "schedule": s["schedule"],                 # from build_schedule()
                "active": bool(s.get("active", True)),
                "parameters": {
                    "command": command,
                    "command_type": command_type,
                    "provider": self.provider,
                },
            })

        if not payload:
            raise ValueError("No schedules to create.")

        # Always use plural — Prefect expects an array anyway
        resp = self.api.create_deployment_schedules(dep_id, payload)

        # Return a convenience boolean (any active)
        if isinstance(resp, dict):
            sch = resp.get("schedules")
            if isinstance(sch, list):
                return any(bool(x.get("active")) for x in sch if isinstance(x, dict))
            if isinstance(resp.get("active"), bool):
                return bool(resp["active"])
        elif isinstance(resp, list):
            return any(bool(x.get("active")) for x in resp if isinstance(x, dict))
        return None

    def pause_schedule(self, deployment_id: str) -> None:
        self.api.pause_deployment_schedule(deployment_id)

    def resume_schedule(self, deployment_id: str) -> None:
        self.api.resume_deployment_schedule(deployment_id)

    def delete_active_deployment(self, deployment_id: str) -> None:
        self.api.delete_deployment(deployment_id)

    def format_schedule_msg(self, mode: str, existing: str | None = None) -> str:
        if mode == "ACTIVE":
            return "Schedule ACTIVE; check Prefect UI for details"
        if mode == "PAUSED":
            return "Schedule PAUSED"
        return existing or "No schedule"

    def get_deployment_status_bundle(self, deployment_id: str) -> Tuple[str, Dict[str, Any]]:
        """
        Fetch deployment, compute status + schedule fields.
        Returns (dep_status, fields_for_cfg).
        dep_status may be "DELETED" when the server returns 404.
        """
        try:
            dep_resp = self.api.check_deployment_status(deployment_id)
        except requests.exceptions.HTTPError as e:
            if getattr(e, "response", None) is not None and e.response.status_code == 404:
                # Signal to the UI that the deployment is gone on the server
                return "DELETED", {
                    "deleted_on_server": True,
                    "schedule_active": None,
                    "schedule_paused": None,
                    "schedule_mode": "PRIME",
                    "schedule_msg": "No schedule",
                    "server_schedules": [],
                }
            raise

        dep_status = norm_dep_status_value(dep_resp)

        sched_active, sched_paused, mode = derive_schedule_state_from_deployment(dep_resp)
        fields = {
            "schedule_active": sched_active,
            "schedule_paused": sched_paused,
            "schedule_mode":   mode,
            "schedule_msg":    self.format_schedule_msg(mode),
            "server_schedules": (dep_resp.get("schedules") or []),
        }
        return dep_status, fields

    def try_refresh_deployment_status(self, cfg: dict, throttle_s: float = 3.0) -> str:
        dep_id = cfg.get("deployment_id")
        if not dep_id:
            cfg["last_dep_status"] = "UNKNOWN"
            return "UNKNOWN"

        now = time.time()
        last_ts = cfg.get("last_status_check_ts", 0.0)
        use_cache = (now - last_ts) < throttle_s and "last_dep_status" in cfg
        if use_cache:
            return cfg["last_dep_status"]

        dep_status, fields = self.get_deployment_status_bundle(dep_id)  # this now handles 404→DELETED
        cfg.update(fields)
        cfg["last_dep_status"] = dep_status
        cfg["last_status_check_ts"] = now
        return dep_status
