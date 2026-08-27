"""Async rate limiter enforcing IBKR's historical-data pacing rules (strategy doc Section 5.3).
Route every outgoing historical request through `PacingGovernor.acquire()` -- this is
mandatory, not optional: under Interactive Brokers' strict pacing setting, violating these
limits terminates the API session after 3 violations (error code 100, surfacing as a
`BrokenPipeError`).

Rules (from IBKR's Historical Data Limitations / Pacing Limitations pages -- re-verify against
the live pages if these numbers are ever suspected stale):
- No identical request within 15 seconds.
- No 6+ requests for the same (contract, exchange, tick type) within 2 seconds.
- No more than 60 requests in any rolling 10-minute window; BID_ASK requests count twice.
- Keep simultaneous in-flight historical requests low (3-5), well under IBKR's 50-request cap.

One `PacingGovernor` instance should be shared by every historical request made on a given
connection -- a fresh instance per request defeats the rolling-window and short-window checks
entirely.
"""

import asyncio
import time
from collections import deque
from collections.abc import AsyncIterator, Hashable
from contextlib import asynccontextmanager
from typing import NamedTuple

_ROLLING_WINDOW_S = 600.0
_ROLLING_MAX_REQUESTS = 60
_IDENTICAL_REQUEST_COOLDOWN_S = 15.0
_SHORT_WINDOW_S = 2.0
_SHORT_WINDOW_MAX_REQUESTS = 6
_MAX_IN_FLIGHT = 5


class RequestSignature(NamedTuple):
    """Identifies one historical request. `contract_key`/`exchange`/`tick_type` alone (via
    `short_key`) drive the 2-second short-window rule; the full tuple (including `extra`, e.g.
    bar_size/duration_str/end_datetime) drives the 15-second identical-request rule."""

    contract_key: Hashable
    exchange: str
    tick_type: str
    extra: tuple = ()

    @property
    def short_key(self) -> tuple:
        return (self.contract_key, self.exchange, self.tick_type)


class PacingGovernor:
    def __init__(self) -> None:
        self._rolling: deque[tuple[float, int]] = deque()
        self._last_identical: dict[RequestSignature, float] = {}
        self._short_window: dict[tuple, deque[float]] = {}
        self._in_flight = asyncio.Semaphore(_MAX_IN_FLIGHT)
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def acquire(self, signature: RequestSignature, weight: int = 1) -> AsyncIterator[None]:
        """Block until all pacing constraints permit `signature`'s request, hold the in-flight
        slot for the duration of the `async with` block. `weight` should be 2 for BID_ASK
        requests (they count twice against the rolling 10-minute cap)."""
        await self._in_flight.acquire()
        try:
            while True:
                async with self._lock:
                    now = time.monotonic()
                    wait_s = self._time_until_allowed(signature, weight, now)
                    if wait_s <= 0:
                        self._record(signature, weight, now)
                        break
                await asyncio.sleep(wait_s)
            yield
        finally:
            self._in_flight.release()

    def _time_until_allowed(self, signature: RequestSignature, weight: int, now: float) -> float:
        wait_s = 0.0

        last = self._last_identical.get(signature)
        if last is not None:
            wait_s = max(wait_s, _IDENTICAL_REQUEST_COOLDOWN_S - (now - last))

        short_dq = self._short_window.setdefault(signature.short_key, deque())
        while short_dq and now - short_dq[0] > _SHORT_WINDOW_S:
            short_dq.popleft()
        if len(short_dq) >= _SHORT_WINDOW_MAX_REQUESTS:
            wait_s = max(wait_s, _SHORT_WINDOW_S - (now - short_dq[0]))

        while self._rolling and now - self._rolling[0][0] > _ROLLING_WINDOW_S:
            self._rolling.popleft()
        rolling_sum = sum(w for _, w in self._rolling)
        if rolling_sum + weight > _ROLLING_MAX_REQUESTS:
            wait_s = max(wait_s, _ROLLING_WINDOW_S - (now - self._rolling[0][0]))

        return wait_s

    def _record(self, signature: RequestSignature, weight: int, now: float) -> None:
        self._last_identical[signature] = now
        # Opportunistic prune so this doesn't grow unbounded over a long-running flow.
        stale = [sig for sig, ts in self._last_identical.items() if now - ts > _IDENTICAL_REQUEST_COOLDOWN_S]
        for sig in stale:
            del self._last_identical[sig]

        self._short_window[signature.short_key].append(now)
        self._rolling.append((now, weight))
