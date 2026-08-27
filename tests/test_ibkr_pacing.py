import asyncio

import pytest

from stockops.data.ibkr.pacing import PacingGovernor, RequestSignature


@pytest.mark.asyncio
async def test_identical_signature_waits_out_the_cooldown(monkeypatch):
    governor = PacingGovernor()
    sig = RequestSignature("AAPL", "SMART", "TRADES", extra=("1 day", "1 D", "20240101-00:00:00"))

    clock = [0.0]
    monkeypatch.setattr("stockops.data.ibkr.pacing.time.monotonic", lambda: clock[0])

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr("stockops.data.ibkr.pacing.asyncio.sleep", fake_sleep)

    async with governor.acquire(sig):
        pass

    clock[0] += 1.0  # well inside the 15s cooldown
    async with governor.acquire(sig):
        pass

    assert sleeps, "second identical request should have waited"
    assert sleeps[0] == pytest.approx(14.0)


@pytest.mark.asyncio
async def test_short_window_caps_at_six_requests_per_two_seconds(monkeypatch):
    governor = PacingGovernor()

    clock = [0.0]
    monkeypatch.setattr("stockops.data.ibkr.pacing.time.monotonic", lambda: clock[0])

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr("stockops.data.ibkr.pacing.asyncio.sleep", fake_sleep)

    for i in range(6):
        sig = RequestSignature("AAPL", "SMART", "TRADES", extra=(f"req-{i}",))
        async with governor.acquire(sig):
            pass

    assert not sleeps  # first 6 in the same instant are all allowed

    seventh = RequestSignature("AAPL", "SMART", "TRADES", extra=("req-6",))
    async with governor.acquire(seventh):
        pass

    assert sleeps and sleeps[0] > 0


@pytest.mark.asyncio
async def test_rolling_window_caps_at_sixty_requests_per_ten_minutes(monkeypatch):
    governor = PacingGovernor()

    clock = [0.0]
    monkeypatch.setattr("stockops.data.ibkr.pacing.time.monotonic", lambda: clock[0])

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr("stockops.data.ibkr.pacing.asyncio.sleep", fake_sleep)

    for i in range(60):
        sig = RequestSignature(f"TICKER{i}", "SMART", "TRADES")
        async with governor.acquire(sig):
            pass
        clock[0] += 3.0  # stay outside the 2s short-window rule between distinct tickers

    assert not sleeps

    sig_61 = RequestSignature("OVERFLOW", "SMART", "TRADES")
    async with governor.acquire(sig_61):
        pass

    assert sleeps and sleeps[0] > 0


@pytest.mark.asyncio
async def test_bid_ask_weight_counts_twice_against_rolling_cap(monkeypatch):
    governor = PacingGovernor()

    clock = [0.0]
    monkeypatch.setattr("stockops.data.ibkr.pacing.time.monotonic", lambda: clock[0])

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr("stockops.data.ibkr.pacing.asyncio.sleep", fake_sleep)

    for i in range(30):
        sig = RequestSignature(f"TICKER{i}", "SMART", "BID_ASK")
        async with governor.acquire(sig, weight=2):
            pass
        clock[0] += 3.0

    assert not sleeps  # 30 requests * weight 2 == 60, exactly at the cap

    sig_31 = RequestSignature("OVERFLOW", "SMART", "BID_ASK")
    async with governor.acquire(sig_31, weight=2):
        pass

    assert sleeps and sleeps[0] > 0


@pytest.mark.asyncio
async def test_max_in_flight_limits_concurrency():
    governor = PacingGovernor()
    in_flight = 0
    max_seen = 0

    async def worker(i: int) -> None:
        nonlocal in_flight, max_seen
        sig = RequestSignature(f"T{i}", "SMART", "TRADES")
        async with governor.acquire(sig):
            in_flight += 1
            max_seen = max(max_seen, in_flight)
            await asyncio.sleep(0.01)
            in_flight -= 1

    await asyncio.gather(*(worker(i) for i in range(10)))
    assert max_seen <= 5
