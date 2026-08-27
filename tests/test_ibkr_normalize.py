from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from stockops.data.ibkr.normalize import ibkr_daybar_to_isodate, to_utc_epoch


def test_to_utc_epoch_from_aware_datetime():
    dt = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
    assert to_utc_epoch(dt) == int(dt.timestamp())


def test_to_utc_epoch_from_naive_datetime_assumed_utc():
    dt = datetime(2024, 6, 1, 12, 0, 0)
    assert to_utc_epoch(dt) == int(dt.replace(tzinfo=UTC).timestamp())


def test_to_utc_epoch_from_non_utc_aware_datetime_converts():
    tz = timezone(timedelta(hours=-4))
    dt = datetime(2024, 6, 1, 8, 0, 0, tzinfo=tz)
    assert to_utc_epoch(dt) == int(dt.astimezone(UTC).timestamp())


def test_to_utc_epoch_from_int_seconds():
    assert to_utc_epoch(1717243200) == 1717243200


def test_to_utc_epoch_from_float_milliseconds():
    assert to_utc_epoch(1717243200000.0) == 1717243200


def test_to_utc_epoch_from_numeric_string():
    assert to_utc_epoch("1717243200") == 1717243200


def test_to_utc_epoch_from_iso_string():
    assert to_utc_epoch("2024-06-01T12:00:00+00:00") == int(datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC).timestamp())


def test_to_utc_epoch_rejects_unsupported_type():
    with pytest.raises(TypeError):
        to_utc_epoch(object())  # pyright: ignore[reportArgumentType]


def test_ibkr_daybar_to_isodate_from_date():
    assert ibkr_daybar_to_isodate(date(2024, 6, 1)) == "2024-06-01"


def test_ibkr_daybar_to_isodate_from_datetime():
    assert ibkr_daybar_to_isodate(datetime(2024, 6, 1, 9, 30, tzinfo=UTC)) == "2024-06-01"
