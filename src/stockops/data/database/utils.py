import calendar
from collections.abc import Iterator
from datetime import date, timedelta
from typing import Literal

Precision = Literal["yr", "mo", "day"]
_MONTHS = ("0", "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
MON2NUM = {m: i for i, m in enumerate(_MONTHS) if i > 0}
NUM2MON = {i: m for i, m in enumerate(_MONTHS) if i > 0}


def set_ts_col(provider: str, data_type: str) -> str:
    if provider == "EODHD":
        if data_type == "historical_interday":
            return "date"
        elif data_type == "historical_intraday":
            return "timestamp_UTC_s"
        elif data_type == "streaming":
            return "timestamp_UTC_ms"
        else:
            raise ValueError(f"Unsupported data type: {data_type}")
    else:
        raise ValueError(f"Unknown provider: {provider}")


def period_from_unix(
    start_utcts: tuple[str, ...], end_utcts: tuple[str, ...], precision: Precision
) -> list[tuple[str, ...]]:
    s_yr, s_mon, s_day = start_utcts
    e_yr, e_mon, e_day = end_utcts

    s_t: tuple[str, ...]
    e_t: tuple[str, ...]
    if precision == "yr":
        s_t, e_t = (s_yr,), (e_yr,)
    elif precision == "mo":
        s_t, e_t = (s_yr, s_mon), (e_yr, e_mon)
    else:
        s_t, e_t = (s_yr, s_mon, s_day), (e_yr, e_mon, e_day)

    return list(_date_tuple_range(s_t, e_t, precision=precision))


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


def _date_tuple_range(
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
