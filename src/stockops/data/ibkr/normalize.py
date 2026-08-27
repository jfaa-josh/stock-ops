"""Timestamp normalization for IBKR historical bars and streaming ticks (strategy doc Section 5.2).

Day-bar dates use `ibkr_daybar_to_isodate`, not `to_utc_epoch`. This is a deliberate departure
from the strategy doc's original plan to route date/week/month bars through `to_utc_epoch` as
well: read directly against `stockops.data.transform.TransformData.eodhd`'s
`historical_interday` case, EODHD interday rows store a bare "YYYY-MM-DD" string with no time
or zone at all -- not a UTC epoch. IBKR's future interday transform (Chunk 5) must match that
convention exactly for the two providers' interday rows to stay comparable, so day/week/month
bars need a same-shape string, not an epoch int.

`ib_async.objects.BarData.date` is typed `date | datetime`: a plain `date` for day/week/month
bars (no time, no zone), a tz-aware `datetime` for anything finer (with `reqMarketDataType`'s
formatDate=2, per Section 5.2) -- confirmed directly against the installed ib_async source.
"""

from datetime import UTC, date, datetime

from stockops.data.utils import normalize_ts_to_seconds


def to_utc_epoch(value: datetime | str | int | float) -> int:
    """Normalize an intraday bar timestamp or streaming tick time to UTC epoch seconds. Do not
    use for day/week/month bars -- see `ibkr_daybar_to_isodate`."""
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return int(dt.astimezone(UTC).timestamp())

    if isinstance(value, int | float):
        return int(normalize_ts_to_seconds(value))

    if isinstance(value, str):
        try:
            return int(normalize_ts_to_seconds(float(value)))
        except ValueError:
            pass
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return int(dt.astimezone(UTC).timestamp())

    raise TypeError(f"Unsupported type for to_utc_epoch: {type(value).__name__}")


def ibkr_daybar_to_isodate(value: date | datetime) -> str:
    """Convert an IBKR day/week/month bar's date to this repo's plain "YYYY-MM-DD" interday
    convention (see module docstring)."""
    if isinstance(value, datetime):
        value = value.date()
    return value.isoformat()


def to_utc_epoch_ms(value: datetime) -> int:
    """Normalize a streaming tick's timestamp (`TickByTickAllLast.time` / `TickByTickBidAsk.time`,
    always a tz-aware `datetime`) to UTC epoch milliseconds. Second precision (`to_utc_epoch`)
    isn't enough here -- multiple ticks can land within the same second."""
    dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return int(dt.astimezone(UTC).timestamp() * 1000)
