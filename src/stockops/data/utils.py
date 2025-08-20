import math
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo


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


def utcts_to_tzstr_parsed(value: int | float, tz: ZoneInfo) -> tuple[str, str, str] | tuple:
    """
    If value is an int/float Unix timestamp (assumed UTC), convert to the provided tz
    and return (year, month_name, day) as strings.
    """
    _MONTHS = ("0", "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")

    secs = normalize_ts_to_seconds(value)  # If not in fractional s, convert first
    try:
        dt = datetime.fromtimestamp(secs, tz=UTC).astimezone(tz)
    except OSError as e:
        raise ValueError(f"Out-of-range Unix timestamp after normalization: seconds={secs}") from e

    return (str(dt.year), _MONTHS[dt.month], f"{dt.day:02d}")


def tzstr_to_utcts(dt_str: str, format: str, tz: ZoneInfo) -> int:
    return int(datetime.strptime(dt_str, format).replace(tzinfo=tz).timestamp())


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
