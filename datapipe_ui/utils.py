from typing import Dict, List, Tuple, Any, Optional, Mapping
from zoneinfo import ZoneInfo
import datetime as dt
import logging

logger = logging.getLogger(__name__)

def summarize_schedules_for_ui(
    schedules: List[Dict[str, Any]],
    *,
    mode: str = "friendly",       # kept for compatibility but we only use "friendly"
    show_dtstart: bool = False,   # NEW: include DTSTART info inline
) -> str:
    """
    Condense a list of Prefect RRule schedules (new style only) to one line.
    Each item like:
      {"active": true, "schedule": {"rrule": "DTSTART...\\nRRULE:FREQ=...;...", "timezone": "America/New_York"}}
    """

    if not schedules:
        return "—"

    # ---- Extract DTSTART of the first schedule (if present) ----
    # We prefer the explicit timezone field (matches your build_schedule), but also parse TZID in DTSTART if needed.
    first_s = schedules[0].get("schedule") or {}
    tz_hint = (first_s.get("timezone") or "UTC")
    rrule_raw = (first_s.get("rrule") or "").strip()
    lines = [ln.strip() for ln in rrule_raw.splitlines() if ln.strip()]

    dtstart_line = next((ln for ln in lines if ln.startswith("DTSTART")), None)
    dtstart_local_str = None
    if dtstart_line and ":" in dtstart_line:
        # Examples:
        #   DTSTART;TZID=America/New_York:20250814T09300000
        #   DTSTART:20250814T09300000
        lhs, rhs = dtstart_line.split(":", 1)
        tzid = None
        if "TZID=" in lhs:
            try:
                tzid = lhs.split("TZID=", 1)[1]
            except Exception:
                tzid = None
        dtstart_naive = None
        try:
            dtstart_naive = dt.datetime.strptime(rhs, "%Y%m%dT%H%M%S")
        except Exception:
            dtstart_naive = None

        if dtstart_naive:
            tz_use = tzid or tz_hint or "UTC"
            try:
                dtstart_aware = dtstart_naive.replace(tzinfo=ZoneInfo(tz_use))
                dtstart_local_str = dtstart_aware.strftime("%Y-%m-%d %H:%M")
            except Exception:
                # Fallback: print naive local time without tz if ZoneInfo fails
                dtstart_local_str = dtstart_naive.strftime("%Y-%m-%d %H:%M")

    # ---- Friendly summary (your original logic, unchanged) ----
    def _parse_one(s: Dict[str, Any]) -> Dict[str, Any]:
        sch = s.get("schedule") or {}
        tz = sch.get("timezone") or "UTC"
        rrule = (sch.get("rrule") or "").strip()

        # Extract RRULE content for this item
        _lines = [ln.strip() for ln in rrule.splitlines() if ln.strip()]
        _rr_line = next((ln for ln in _lines if ln.startswith("RRULE")), None)
        if _rr_line and "RRULE:" in _rr_line:
            rr = _rr_line.split("RRULE:", 1)[1].strip()
        else:
            rr = rrule  # best-effort (old style)

        parts: Dict[str, str] = {}
        for chunk in rr.split(";"):
            chunk = chunk.strip()
            if "=" in chunk:
                k, v = chunk.split("=", 1)
                parts[k] = v

        freq = parts.get("FREQ")
        try:
            interval = int(parts.get("INTERVAL", "1"))
        except ValueError:
            interval = 1

        byday = parts.get("BYDAY")

        def _ints(key: str) -> list[int]:
            if key not in parts:
                return []
            out: list[int] = []
            for tok in parts[key].split(","):
                tok = tok.strip()
                if tok:
                    try:
                        out.append(int(tok))
                    except ValueError:
                        pass
            return out

        byhour   = _ints("BYHOUR")
        byminute = _ints("BYMINUTE") or [0]
        bysecond = _ints("BYSECOND") or [0]

        # UNTIL: if UTC (ends with Z), convert to local; else treat as local wall time
        until_local = None
        u = parts.get("UNTIL")
        if u:
            try:
                if u.endswith("Z"):
                    u_dt = dt.datetime.strptime(u, "%Y%m%dT%H%M%SZ").replace(tzinfo=ZoneInfo("UTC"))
                    until_local = u_dt.astimezone(ZoneInfo(tz))
                else:
                    until_local = dt.datetime.strptime(u, "%Y%m%dT%H%M%S").replace(tzinfo=ZoneInfo(tz))
            except Exception:
                until_local = None

        return {
            "tz": tz,
            "freq": freq,
            "interval": interval,
            "byday": byday,
            "byhour": byhour,
            "byminute": byminute,
            "bysecond": bysecond,
            "until_local": until_local,
        }

    parsed = [_parse_one(s) for s in schedules]

    def _key_without_hours(p: Dict[str, Any]) -> Tuple:
        return (
            p.get("tz"),
            p.get("freq"),
            p.get("interval"),
            p.get("byday") or "",
            tuple(p.get("byminute") or [0]),
            tuple(p.get("bysecond") or [0]),
            p["until_local"].strftime("%Y-%m-%d %H:%M:%S%z") if p.get("until_local") else "",
        )

    groups: Dict[Tuple, Dict[str, Any]] = {}
    for p in parsed:
        k = _key_without_hours(p)
        if k not in groups:
            groups[k] = {**p, "hours": []}
        groups[k]["hours"].extend(p["byhour"])

    if len(groups) > 1:
        friendly = f"Mixed schedules ({len(schedules)})"
    else:
        g = next(iter(groups.values()))
        tz = g["tz"]
        freq = g.get("freq")
        interval = g.get("interval", 1)
        byday = g.get("byday")
        mins = g.get("byminute") or [0]
        secs = g.get("bysecond") or [0]
        until_local = g.get("until_local")
        hours_sorted = sorted(set(g["hours"]))

        unit_map = {"DAILY":"day","WEEKLY":"week","MONTHLY":"month","YEARLY":"year","HOURLY":"hour","MINUTELY":"minute"}
        title_map = {"DAILY":"Daily","WEEKLY":"Weekly","MONTHLY":"Monthly","YEARLY":"Yearly","HOURLY":"Hourly","MINUTELY":"Minutely"}

        unit = unit_map.get(freq or "", "run")
        freq_str = title_map.get(freq or "", "Recurring") if interval == 1 else f"Every {interval} {unit}s"

        if byday:
            day_map = {"MO":"Mon","TU":"Tue","WE":"Wed","TH":"Thu","FR":"Fri","SA":"Sat","SU":"Sun"}
            days_list: list[str] = [day_map[d] if d in day_map else d for d in byday.split(",")]
            days_str = ", ".join(days_list)
            freq_str = f"{freq_str} ({days_str})"

        times_str = ""
        if freq in {"DAILY","WEEKLY","MONTHLY","YEARLY"}:
            mm = mins[0]
            times_list: list[str] = [f"{h:02d}:{mm:02d}" for h in hours_sorted]
            if times_list:
                times_str = " @ " + ", ".join(times_list)
        elif freq == "HOURLY":
            mm, ss = mins[0], secs[0]
            if mm or ss:
                base = f" @ minute {mm:02d}"
                times_str = base + (f", second {ss:02d}" if ss else "")
        elif freq == "MINUTELY":
            ss = secs[0]
            if ss:
                times_str = f" @ second {ss:02d}"

        until_str = f" until {until_local.strftime('%Y-%m-%d')}" if until_local else ""
        friendly = f"{freq_str}{times_str} {until_str}"

    # Append DTSTART inline if requested
    if show_dtstart and dtstart_local_str:
        # Append succinctly (keeps single-line layout)
        friendly = f"{friendly} | starts {dtstart_local_str}"

    return friendly

def parse_times_csv(s: str) -> list[dt.time]:
    times: list[dt.time] = []
    for token in (t.strip() for t in s.split(",") if t.strip()):
        try:
            hh_str, mm_str = token.split(":")
            hh, mm = int(hh_str), int(mm_str)
            if not (0 <= hh <= 23 and 0 <= mm <= 59):
                raise ValueError
            times.append(dt.time(hh, mm))
        except Exception:
            raise ValueError(f"Invalid time '{token}'. Use 24h HH:MM, e.g., 15:00")
    if not times:
        raise ValueError("Provide at least one time.")
    return times

def norm_dep_status_value(dep_resp: Any) -> str:
    # Fast-path: dict-like response
    if isinstance(dep_resp, Mapping):
        s = dep_resp.get("status")
        if isinstance(s, dict):
            inner = s.get("status")
            return inner if isinstance(inner, str) else "NOT_READY"
        if isinstance(s, str):
            return s
        return "NOT_READY"

    # Sometimes a backend mistakenly returns just the status string
    if isinstance(dep_resp, str):
        logger.debug("norm_dep_status_value: got raw string %r", dep_resp)
        return dep_resp

    # Anything else is unexpected; don't crash the UI
    logger.debug("norm_dep_status_value: unexpected type %s", type(dep_resp).__name__)
    return "NOT_READY"

def derive_schedule_state_from_deployment(dep: dict[str, Any]) -> tuple[Optional[bool], Optional[bool], str]:
    """
    Returns (schedule_active, schedule_paused, mode)
      - schedule_active: True/False/None
      - schedule_paused: True/False/None
      - mode: "ACTIVE" | "PAUSED" | "PRIME"
    """
    if not isinstance(dep, dict):
        return None, None, "PRIME"

    paused = bool(dep.get("paused"))
    schedules = dep.get("schedules") or []
    any_active = any(bool(s.get("active")) for s in schedules if isinstance(s, dict))

    if paused:
        return False, True, "PAUSED"
    if any_active:
        return True, False, "ACTIVE"
    # exists but not active, or no schedules at all
    return False if schedules else None, False, "PRIME"
