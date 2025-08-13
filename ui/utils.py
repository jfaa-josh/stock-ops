from typing import Dict, List, Tuple, Any
from zoneinfo import ZoneInfo
import datetime as dt

def summarize_schedules_for_ui(schedules: List[Dict[str, Any]]) -> str:
    """
    Condense a list of Prefect RRule schedules (new style only) to one line.
    Expected each item to have:
        {"active": true, "schedule": {"rrule": "FREQ=...;...", "timezone": "America/New_York"}}
    """
    if not schedules:
        return "—"

    def _parse_one(s: Dict[str, Any]) -> Dict[str, Any]:
        sch = s.get("schedule") or {}
        tz = sch.get("timezone") or "UTC"
        rrule = (sch.get("rrule") or "").strip()

        # Parse key=value;key=value
        parts: Dict[str, str] = {}
        for chunk in rrule.split(";"):
            chunk = chunk.strip()
            if "=" in chunk:
                k, v = chunk.split("=", 1)
                parts[k] = v

        freq = parts.get("FREQ")
        try:
            interval = int(parts.get("INTERVAL", "1"))
        except ValueError:
            interval = 1

        byday = parts.get("BYDAY")  # e.g. "MO,WE,FR" or None

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
            "byday": byday,          # string or None
            "byhour": byhour,        # list[int]
            "byminute": byminute,    # list[int], default [0]
            "bysecond": bysecond,    # list[int], default [0]
            "until_local": until_local,  # aware or None
        }

    parsed = [_parse_one(s) for s in schedules]

    # Group schedules that only differ by BYHOUR (so we can show multiple times succinctly)
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
        return f"Mixed schedules ({len(schedules)})"

    # Single group → build a readable string
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

    return f"{freq_str}{times_str} ({tz}){until_str}"

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
