from datetime import UTC, datetime

import pytest

from stockops.data.streaming.ibkr_streaming_service import (
    _check_restart_boundary,
    _next_restart_utc,
    _tick_to_data_row,
)


def test_next_restart_utc_same_day_when_before_restart():
    # 2026-08-27 05:00 UTC = 2026-08-27 01:00 America/New_York -- before the 02:00 restart.
    now_utc = datetime(2026, 8, 27, 5, 0, tzinfo=UTC)
    restart_at = _next_restart_utc(now_utc)
    assert restart_at == datetime(2026, 8, 27, 6, 0, tzinfo=UTC)  # 02:00 EDT == 06:00 UTC


def test_next_restart_utc_next_day_when_after_restart():
    # 2026-08-27 12:00 UTC = 2026-08-27 08:00 America/New_York -- already past the 02:00 restart.
    now_utc = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)
    restart_at = _next_restart_utc(now_utc)
    assert restart_at == datetime(2026, 8, 28, 6, 0, tzinfo=UTC)


def test_check_restart_boundary_passes_for_short_run_well_before_restart():
    now_utc = datetime(2026, 8, 27, 5, 0, tzinfo=UTC)  # 01:00 America/New_York
    _check_restart_boundary(60, now=now_utc)  # 1 minute -- nowhere near the 06:00 UTC restart


def test_check_restart_boundary_raises_when_run_would_cross_restart():
    now_utc = datetime(2026, 8, 27, 5, 0, tzinfo=UTC)  # 01:00 America/New_York
    with pytest.raises(RuntimeError):
        _check_restart_boundary(3600, now=now_utc)  # 1 hour -- crosses the 06:00 UTC restart


def test_check_restart_boundary_raises_inside_guard_window():
    # Ends 2 minutes before the restart -- inside the 300s guard window, should still raise.
    now_utc = datetime(2026, 8, 27, 5, 55, tzinfo=UTC)
    with pytest.raises(RuntimeError):
        _check_restart_boundary(120, now=now_utc)


def test_tick_to_data_row_last():
    tick = type("Tick", (), {"time": "t", "price": 1.0, "size": 5.0})()
    assert _tick_to_data_row(tick, "Last") == {"time": "t", "price": 1.0, "size": 5.0}


def test_tick_to_data_row_bidask():
    tick = type("Tick", (), {"time": "t", "bidPrice": 1.0, "askPrice": 1.1, "bidSize": 2.0, "askSize": 3.0})()
    assert _tick_to_data_row(tick, "BidAsk") == {
        "time": "t",
        "bidPrice": 1.0,
        "askPrice": 1.1,
        "bidSize": 2.0,
        "askSize": 3.0,
    }
