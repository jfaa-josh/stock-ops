from datetime import UTC, datetime

import pytest

from stockops.data.ibkr.bars import BAR_SIZE_SETTINGS, generate_windows


def test_unknown_bar_size_raises():
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 2, tzinfo=UTC)
    with pytest.raises(ValueError):
        generate_windows(start, end, "not a real bar size")


def test_requires_timezone_aware_bounds():
    end = datetime(2024, 1, 2, tzinfo=UTC)
    with pytest.raises(ValueError):
        generate_windows(datetime(2024, 1, 1), end, "1 day")


def test_end_must_be_after_start():
    dt = datetime(2024, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError):
        generate_windows(dt, dt, "1 day")


def test_single_chunk_when_span_fits():
    start = datetime(2024, 3, 1, 9, 0, tzinfo=UTC)
    end = datetime(2024, 3, 1, 16, 0, tzinfo=UTC)
    windows = generate_windows(start, end, "1 min")

    assert len(windows) == 1
    end_dt_str, duration_str = windows[0]
    assert duration_str == "1 D"
    assert end_dt_str == "20240301-16:00:00"


def test_multi_chunk_walks_backward_and_covers_start():
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 10, tzinfo=UTC)
    windows = generate_windows(start, end, "1 hour")

    # Each window's endDateTime must be strictly decreasing (walking backward from `end`).
    end_dts = [datetime.strptime(w[0], "%Y%m%d-%H:%M:%S").replace(tzinfo=UTC) for w in windows]
    assert end_dts == sorted(end_dts, reverse=True)
    assert end_dts[0] == end

    # The earliest window's end anchor, minus its 1-day duration, must reach back to (or past)
    # `start` -- i.e. the windows fully cover the requested span with no gap.
    assert end_dts[-1] - _duration_timedelta(windows[-1][1]) <= start


def test_consecutive_windows_overlap_by_one_bar():
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 5, tzinfo=UTC)
    windows = generate_windows(start, end, "1 hour")

    end_dts = [datetime.strptime(w[0], "%Y%m%d-%H:%M:%S").replace(tzinfo=UTC) for w in windows]
    assert len(end_dts) >= 2

    step = end_dts[0] - end_dts[1]
    chunk = _duration_timedelta(windows[0][1])
    bar_length = chunk - step
    assert bar_length.total_seconds() > 0


def test_every_interval_mapping_has_a_chunk_duration():
    from stockops.data.ibkr.bars import PREFERRED_CHUNK_DURATION

    for bar_size in BAR_SIZE_SETTINGS.values():
        assert bar_size in PREFERRED_CHUNK_DURATION


def _duration_timedelta(duration_str: str):
    from datetime import timedelta

    n_str, unit = duration_str.split()
    n = int(n_str)
    if unit == "D":
        return timedelta(days=n)
    if unit == "Y":
        return timedelta(days=365 * n)
    raise ValueError(f"Unhandled duration unit in test helper: {duration_str!r}")
