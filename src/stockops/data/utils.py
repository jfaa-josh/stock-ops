import calendar
import math
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

_MONTHS = ("0", "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
Precision = Literal["yr", "mo", "day"]
MON2NUM = {m: i for i, m in enumerate(_MONTHS) if i > 0}
NUM2MON = {i: m for i, m in enumerate(_MONTHS) if i > 0}


def normalize_ts_to_seconds(value: int | float) -> float:
    """
    Normalize Unix time to *seconds* (float). Accepts s, ms, or µs based on magnitude.
    ~1.7e9 s (2025), ~1.7e12 ms, ~1.7e15 µs. Uses loose thresholds.
    """
    if not isinstance(value, int | float) or not math.isfinite(value):
        raise TypeError(f"Unsupported or non-finite type/value: {value!r}")

    v = float(value)
    av = abs(v)
    # Heuristics: >=1e14 -> µs, >=1e11 -> ms, else seconds
    if av >= 1e14:
        return v / 1_000_000.0  # microseconds -> seconds
    if av >= 1e11:
        return v / 1_000.0  # milliseconds -> seconds
    return v  # seconds


def utcts_to_tzstr_parsed(value: int | float, tz: ZoneInfo) -> tuple[str, str, str] | tuple[str, str, str]:
    """
    If value is an int/float Unix timestamp (assumed UTC), convert to the provided tz
    and return (year, month_name, day) as strings.
    """

    secs = normalize_ts_to_seconds(value)  # If not in fractional s, convert first
    try:
        dt = datetime.fromtimestamp(secs, tz=UTC).astimezone(tz)
    except OSError as e:
        raise ValueError(f"Out-of-range Unix timestamp after normalization: seconds={secs}") from e

    return (str(dt.year), _MONTHS[dt.month], f"{dt.day:02d}")


def tzstr_to_utcts(dt_str: str, format: str, tz: ZoneInfo) -> int:
    return int(datetime.strptime(dt_str, format).replace(tzinfo=tz).timestamp())


def utcts_to_tzstr(ts: int | float, format: str, tz: ZoneInfo) -> str:
    return datetime.fromtimestamp(ts, tz=tz).strftime(format)


def validate_isodatestr(s: str) -> str:
    """
    Parse a date in “YYYY-MM-DD” form and return a date object.
    Raises ValueError if the format is wrong or the date is invalid.
    """
    date.fromisoformat(s)
    return s


def validate_utc_ts(ts: int, precision: str) -> int:
    """
    Ensure ts is an integer Unix timestamp in UTC.
    If precision is 's', ts is in seconds; if 'ms', ts is in milliseconds.
    """
    if not isinstance(ts, int):
        raise TypeError(f"Timestamp must be int, got {type(ts).__name__}")

    if precision == "s":
        datetime.fromtimestamp(ts, tz=UTC)
    elif precision == "ms":
        datetime.fromtimestamp(ts / 1000.0, tz=UTC)
    else:
        raise ValueError(f"Unsupported precision {precision!r}, expected 's' or 'ms'")
    return ts


def _parse_tuple(t: tuple[str, ...], precision: Precision) -> tuple[int, int, int]:
    """Return (Y,M,D) with sensible defaults based on precision."""
    if precision == "yr":
        if len(t) != 1:
            raise ValueError("yr precision expects ('YYYY',)")
        y = int(t[0])
        return (y, 1, 1)

    if precision == "mo":
        if len(t) != 2:
            raise ValueError("mo precision expects ('YYYY','MON')")
        y, mon = int(t[0]), t[1].upper()
        if mon not in MON2NUM:
            raise ValueError(f"Unknown month token: {mon}")
        return (y, MON2NUM[mon], 1)

    # precision == "day"
    if len(t) != 3:
        raise ValueError("day precision expects ('YYYY','MON','DD')")
    y, mon, d = int(t[0]), t[1].upper(), int(t[2])
    if mon not in MON2NUM:
        raise ValueError(f"Unknown month token: {mon}")
    return (y, MON2NUM[mon], d)


def _end_cap(y: int, m: int, precision: Precision) -> tuple[int, int, int]:
    """Expand the end bound to the end of the unit for inclusivity (yr/mo only)."""
    if precision == "yr":
        return (y, 12, 31)
    if precision == "mo":
        return (y, m, calendar.monthrange(y, m)[1])
    # Not used for "day" in current flow.
    return (y, m, 0)


def date_tuple_range(
    start_t: tuple[str, ...], end_t: tuple[str, ...], precision: Precision
) -> Iterator[tuple[str, ...]]:
    """
    Inclusive generator at the chosen precision.
    Returns ('YYYY',) | ('YYYY','MON') | ('YYYY','MON','DD').
    """
    ys, ms, ds = _parse_tuple(start_t, precision)
    ye, me, de = _parse_tuple(end_t, precision)

    # Build concrete inclusive date bounds
    start_date = date(ys, ms, ds)
    if precision == "day":
        end_date = date(ye, me, de)
    else:
        ye2, me2, de2 = _end_cap(ye, me, precision)
        end_date = date(ye2, me2, de2)

    if start_date > end_date:
        start_date, end_date = end_date, start_date  # swap

    if precision == "yr":
        for y in range(start_date.year, end_date.year + 1):
            yield (str(y),)

    elif precision == "mo":
        y, m = start_date.year, start_date.month
        while (y < end_date.year) or (y == end_date.year and m <= end_date.month):
            yield (str(y), NUM2MON[m])
            m += 1
            if m > 12:
                m = 1
                y += 1

    else:  # "day"
        cur = start_date
        step = timedelta(days=1)
        while cur <= end_date:
            yield (str(cur.year), NUM2MON[cur.month], f"{cur.day:02d}")
            cur += step


def period_from_unix(
    start_utcts: int | float, end_utcts: int | float, tz, precision: Precision
) -> list[tuple[str, ...]]:
    s_yr, s_mon, s_day = utcts_to_tzstr_parsed(start_utcts, tz)
    e_yr, e_mon, e_day = utcts_to_tzstr_parsed(end_utcts, tz)

    s_t: tuple[str, ...]
    e_t: tuple[str, ...]
    if precision == "yr":
        s_t, e_t = (s_yr,), (e_yr,)
    elif precision == "mo":
        s_t, e_t = (s_yr, s_mon), (e_yr, e_mon)
    else:
        s_t, e_t = (s_yr, s_mon, s_day), (e_yr, e_mon, e_day)

    return list(date_tuple_range(s_t, e_t, precision=precision))
