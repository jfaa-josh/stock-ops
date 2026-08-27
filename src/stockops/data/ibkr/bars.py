"""Historical-bar constants and request windowing (strategy doc Sections 4, 5.1, 5.2).

`BAR_SIZE_SETTINGS` maps this repo's `interval` command values to the literal IBKR
`barSizeSetting` strings `reqHistoricalData` expects -- wire-protocol literals, picky about
spelling ("5 mins" plural, "1 hour" singular). Never build them by string formatting.

`PREFERRED_CHUNK_DURATION` picks one conservative `durationStr` per bar size to walk backward
in `generate_windows`. It is deliberately *not* sourced from IBKR's Max Duration Per Bar Size
page: fetched live (not from memory), that page lists an identical, flat ceiling
(86400 S / 365 D / 52 W / 12 M / 68 Y) for every bar size from 5 secs up to 1 month, which does
not describe a real per-bar-size constraint. Instead these chunk sizes follow the Step Sizes
page's duration-unit -> bar-size compatibility ranges (a stable, sane table): a "D" (day)
duration accepts bar sizes from 5 secs to 1 hour, and a "Y" (year) duration accepts bar sizes
from 1 min to 1 day. Any request that turns out to be oversized in practice surfaces as error
code 162 (empty/no-data result, Section 5.5) rather than a silent gap.
"""

from datetime import UTC, datetime, timedelta

BAR_SIZE_SETTINGS: dict[str, str] = {
    "1m": "1 min",
    "5m": "5 mins",
    "1h": "1 hour",
    "d": "1 day",
    "w": "1 week",
    "m": "1 month",
}

# durationStr to request per window, keyed by barSizeSetting.
PREFERRED_CHUNK_DURATION: dict[str, str] = {
    "1 min": "1 D",
    "5 mins": "1 D",
    "1 hour": "1 D",
    "1 day": "1 Y",
    "1 week": "1 Y",
    "1 month": "1 Y",
}

# Wall-clock length of the chunk duration strings above, for stepping the backward walk.
_CHUNK_STEP: dict[str, timedelta] = {
    "1 D": timedelta(days=1),
    "1 Y": timedelta(days=365),
}

# Approximate length of one bar, used only to size the one-bar overlap between windows
# ("Overlap windows by one bar and let the SQLite dedup layer absorb the duplicate" --
# Section 5.1). The 30-day month approximation is fine: overlap slop is absorbed the same way
# regardless of exactness.
_BAR_LENGTH: dict[str, timedelta] = {
    "1 min": timedelta(minutes=1),
    "5 mins": timedelta(minutes=5),
    "1 hour": timedelta(hours=1),
    "1 day": timedelta(days=1),
    "1 week": timedelta(weeks=1),
    "1 month": timedelta(days=30),
}


def generate_windows(start: datetime, end: datetime, bar_size: str) -> list[tuple[str, str]]:
    """Split [start, end] into an ordered list of (endDateTime, durationStr) request windows
    for `reqHistoricalData`, walking backward from `end`. Consecutive windows overlap by one
    bar (see module docstring). `start`/`end` must be timezone-aware.

    The earliest window may extend slightly before `start` -- callers should filter returned
    rows against `start` themselves rather than relying on exact alignment here.
    """
    if bar_size not in PREFERRED_CHUNK_DURATION:
        raise ValueError(f"Unknown bar_size: {bar_size!r}; expected one of {sorted(PREFERRED_CHUNK_DURATION)}")
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start and end must be timezone-aware")
    if end <= start:
        raise ValueError(f"end ({end}) must be after start ({start})")

    duration_str = PREFERRED_CHUNK_DURATION[bar_size]
    step = _CHUNK_STEP[duration_str]
    overlap = _BAR_LENGTH[bar_size]

    windows: list[tuple[str, str]] = []
    anchor = end
    while anchor > start:
        windows.append((_format_end_datetime(anchor), duration_str))
        anchor -= step - overlap

    return windows


def _format_end_datetime(value: datetime) -> str:
    """IBKR's `endDateTime` UTC form: `yyyymmdd-hh:mm:ss` (Section 5.2)."""
    return value.astimezone(UTC).strftime("%Y%m%d-%H:%M:%S")
