import math
from datetime import UTC, date, datetime
from typing import TypedDict, cast
from zoneinfo import ZoneInfo

from stockops.data.database.utils import period_from_unix

_MONTHS = ("0", "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")


class DBFilenameParts(TypedDict):
    data_type: str
    provider: str
    exchange: str
    year: str | None
    month: str | None
    day: str | None


def parse_db_filename(filename: str) -> DBFilenameParts:
    if not filename.endswith(".db"):
        raise ValueError(f"Not a valid DB filename: {filename}")
    parts = filename[:-3].split("_")

    if parts[0] == "historical":
        data_type = "_".join(parts[:2])
        parts = parts[2:]
    else:
        data_type = parts[0]
        parts = parts[1:]

    provider, exchange, *rest = parts
    year = rest[0] if len(rest) >= 1 else None
    month = rest[1] if len(rest) >= 2 else None
    day = rest[2] if len(rest) >= 3 else None

    return {
        "data_type": data_type,
        "provider": provider,
        "exchange": exchange,
        "year": year,
        "month": month,
        "day": day,
    }


def get_standard_db_filename(
    data_type: str, provider: str, exchange: str, y: str | None = None, m: str | None = None, d: str | None = None
) -> str:
    if data_type == "historical_interday":
        return f"{data_type}_{provider}_{exchange}.db"

    elif data_type == "historical_intraday" and all([y, m]):
        return f"{data_type}_{provider}_{exchange}_{y}_{m}.db"

    elif data_type == "streaming" and all([y, m, d]):
        return f"{data_type}_{provider}_{exchange}_{y}_{m}_{d}.db"

    raise ValueError(f"Unsupported data_type: {data_type!r}")


def get_db_filename_for_date(
    data_type: str, tz: ZoneInfo, provider: str, exchange: str, entry_datetime: int | None
) -> str:
    if data_type == "historical_interday":
        return get_standard_db_filename(data_type, provider, exchange)

    elif data_type == "historical_intraday" and entry_datetime is not None:
        y, m, d = utcts_to_tzstr_parsed(entry_datetime, tz)
        return get_standard_db_filename(data_type, provider, exchange, y, m)

    elif data_type == "streaming" and entry_datetime is not None:
        y, m, d = utcts_to_tzstr_parsed(entry_datetime, tz)
        return get_standard_db_filename(data_type, provider, exchange, y, m, d)

    raise ValueError(f"Unsupported data_type: {data_type!r}")


def get_filenames_for_dates(
    data_type: str, tz: ZoneInfo, provider: str, exchange: str, daterange_endpts: tuple[str | int, str | int]
) -> list[str]:
    if data_type == "historical_interday":
        return [get_standard_db_filename(data_type, provider, exchange)]

    elif data_type == "historical_intraday":
        start, end = cast(tuple[int, int], daterange_endpts)
        start_tup = utcts_to_tzstr_parsed(start, tz)
        end_tup = utcts_to_tzstr_parsed(end, tz)
        filename_dates = period_from_unix(start_tup, end_tup, precision="mo")
        return [get_standard_db_filename(data_type, provider, exchange, y, m) for (y, m) in filename_dates]

    elif data_type == "streaming":
        start, end = cast(tuple[int, int], daterange_endpts)
        start_tup = utcts_to_tzstr_parsed(start, tz)
        end_tup = utcts_to_tzstr_parsed(end, tz)
        filename_dates = period_from_unix(start_tup, end_tup, precision="day")
        return [get_standard_db_filename(data_type, provider, exchange, y, m, d) for (y, m, d) in filename_dates]

    raise ValueError(f"Unsupported data_type: {data_type!r}")


def normalize_ts_to_seconds(value: int | float) -> float:
    """
    Normalize Unix time to seconds (float).
    Detects seconds, milliseconds, or microseconds based on magnitude.
    - ~1e9  (seconds since epoch today)
    - ~1e12 (milliseconds)
    - ~1e15 (microseconds)
    """
    if not isinstance(value, (int | float)) or not math.isfinite(value):
        raise TypeError(f"Unsupported or non-finite type/value: {value!r}")

    v = float(value)
    av = abs(v)

    if av >= 1e14:  # µs
        return v / 1_000_000.0
    elif av >= 1e11:  # ms
        return v / 1_000.0
    else:  # s
        return v


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
    n_ts = normalize_ts_to_seconds(int(ts))
    return datetime.fromtimestamp(n_ts, tz=tz).strftime(format)


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
