import asyncio
import logging
import os
import random
from datetime import UTC, datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

from stockops.config import config, ibkr_config
from stockops.data.database.write_buffer import emit
from stockops.data.transform import TransformData
from stockops.data.utils import get_db_filename_for_date
from stockops.ibkr import contracts
from stockops.ibkr.connection import ibkr_session

from .base_streaming_service import AbstractStreamingService

logger = logging.getLogger(__name__)

_TICK_TYPE_BY_STREAM_TYPE = {"trades": "Last", "quotes": "BidAsk"}

# IBKR data-farm connectivity codes, surfaced via errorEvent (distinct from the client-socket
# disconnectedEvent) -- Section 6.5. 1101: restored with data lost -> must re-subscribe.
# 1102: restored, data maintained -> no action needed.
_RECONNECT_DATA_LOST = 1101
_RECONNECT_DATA_MAINTAINED = 1102

_RESUBSCRIBE = object()


def _tick_to_data_row(tick, tick_type: str) -> dict:
    if tick_type == "Last":
        return {"time": tick.time, "price": tick.price, "size": tick.size}
    return {
        "time": tick.time,
        "bidPrice": tick.bidPrice,
        "askPrice": tick.askPrice,
        "bidSize": tick.bidSize,
        "askSize": tick.askSize,
    }


def _next_restart_utc(now_utc: datetime) -> datetime:
    tz = ZoneInfo(ibkr_config.IBKR_GATEWAY_RESTART_TZ)
    restart_time = dt_time.fromisoformat(ibkr_config.IBKR_GATEWAY_RESTART_LOCAL)
    now_local = now_utc.astimezone(tz)
    candidate = now_local.replace(hour=restart_time.hour, minute=restart_time.minute, second=0, microsecond=0)
    if candidate <= now_local:
        candidate += timedelta(days=1)
    return candidate.astimezone(UTC)


def _check_restart_boundary(duration_s: float, now: datetime | None = None) -> None:
    """Fail fast if this run's [now, now+duration] window would cross the Gateway's daily
    auto-restart -- a silent mid-stream disconnect is much worse than an upfront error naming
    the boundary (Section 6.4)."""
    now = now if now is not None else datetime.now(UTC)
    restart_at = _next_restart_utc(now)
    guard = timedelta(seconds=ibkr_config.IBKR_GATEWAY_RESTART_GUARD_S)
    run_end = now + timedelta(seconds=duration_s)
    if run_end >= restart_at - guard:
        raise RuntimeError(
            f"Requested stream duration ({duration_s:.0f}s) would cross the ib-gateway daily restart "
            f"window at {restart_at.isoformat()} (local {ibkr_config.IBKR_GATEWAY_RESTART_LOCAL} "
            f"{ibkr_config.IBKR_GATEWAY_RESTART_TZ}). Split into separate runs on either side of the boundary."
        )


class IBKRStreamingService(AbstractStreamingService):
    def __init__(self):
        pass

    def write_data(self, table_name: str, exchange: str, transformed_row: dict, test_mode: str, data_type: str):
        filename = get_db_filename_for_date("streaming", self.tz, "IBKR", exchange, transformed_row["timestamp_UTC_ms"])
        db_path = Path(config.RAW_STREAMING_DIR) / str(filename)

        if test_mode == "false":
            emit({"db_path": db_path, "table": table_name, "row": transformed_row})
        elif test_mode == "local":
            print({"db_path": db_path, "table": table_name, "row": transformed_row})
        elif test_mode == "ci":
            if data_type == "streaming_trades":
                expected = [("timestamp_UTC_ms", int), ("price", float), ("volume", int)]
            elif data_type == "streaming_quotes":
                expected = [
                    ("timestamp_UTC_ms", int),
                    ("ask_price", float),
                    ("bid_price", float),
                    ("ask_size", int),
                    ("bid_size", int),
                ]

            assert len(transformed_row) == len(expected), "Length of transformed != length of expected"
            for key, expected_type in expected:
                assert key in transformed_row, f"Missing key {key} in {data_type} data"
                assert isinstance(transformed_row[key], expected_type), (
                    f"Key {key} has type {type(transformed_row[key]).__name__}, expected {expected_type.__name__}"
                )

    def _run_ci(self, data_type: str, ticker: str, exchange: str) -> None:
        transform = TransformData("IBKR", data_type, "to_db_writer", exchange)

        if data_type == "streaming_trades":
            data_row = {"time": datetime(2025, 7, 2, 13, 30, tzinfo=UTC), "price": 657.5311, "size": 5.0}
        elif data_type == "streaming_quotes":
            data_row = {
                "time": datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
                "bidPrice": 657.5421,
                "askPrice": 657.6079,
                "bidSize": 6.0,
                "askSize": 5.0,
            }
        else:
            raise ValueError(f"Unknown data_type for CI mock: {data_type}")

        logger.debug("[%s.%s] TEST_CI mock data: %s", ticker, exchange, data_row)
        transformed_row = transform(data_row)
        self.write_data(ticker, exchange, transformed_row, "ci", data_type)

    async def _run_session(
        self,
        ticker: str,
        exchange: str,
        tick_type: str,
        data_type: str,
        transform: TransformData,
        test_mode: str,
        duration_s: float,
    ) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        disconnected = asyncio.Event()

        async with ibkr_session("streaming") as ib:
            contract = await contracts.qualify_stock(ib, ticker, exchange)

            def on_update(t) -> None:
                if t.tickByTicks:
                    queue.put_nowait(t.tickByTicks[-1])

            def on_error(reqId: int, errorCode: int, errorString: str, errContract) -> None:
                if errorCode == _RECONNECT_DATA_LOST:
                    logger.warning(
                        "[%s.%s] Data-farm connectivity restored with data lost; re-subscribing.", ticker, exchange
                    )
                    queue.put_nowait(_RESUBSCRIBE)
                elif errorCode == _RECONNECT_DATA_MAINTAINED:
                    logger.info("[%s.%s] Data-farm connectivity restored; subscription maintained.", ticker, exchange)

            def on_disconnect() -> None:
                disconnected.set()

            ib.errorEvent += on_error
            ib.disconnectedEvent += on_disconnect

            ticker_obj = ib.reqTickByTickData(contract, tick_type)
            ticker_obj.updateEvent.connect(on_update)

            try:
                loop = asyncio.get_running_loop()
                deadline = loop.time() + duration_s
                while True:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        return
                    if disconnected.is_set():
                        raise ConnectionError("IBKR client disconnected mid-stream")

                    try:
                        tick = await asyncio.wait_for(queue.get(), timeout=remaining)
                    except TimeoutError:
                        return

                    if tick is _RESUBSCRIBE:
                        ib.cancelTickByTickData(contract, tick_type)
                        ticker_obj = ib.reqTickByTickData(contract, tick_type)
                        ticker_obj.updateEvent.connect(on_update)
                        continue

                    data_row = _tick_to_data_row(tick, tick_type)
                    transformed_row = transform(data_row)
                    self.write_data(ticker, exchange, transformed_row, test_mode, data_type)
            finally:
                ib.cancelTickByTickData(contract, tick_type)
                ib.errorEvent -= on_error
                ib.disconnectedEvent -= on_disconnect

    async def _stream_data(self, ticker: str, exchange: str, stream_type: str, duration: float, test_mode: str) -> None:
        data_type = f"streaming_{stream_type}"
        tick_type = _TICK_TYPE_BY_STREAM_TYPE[stream_type]
        transform = TransformData("IBKR", data_type, "to_db_writer", exchange)

        loop = asyncio.get_running_loop()
        started = loop.time()
        backoff = 1.0
        max_backoff = 60.0

        def time_left() -> float:
            return max(0.0, duration - (loop.time() - started))

        while time_left() > 0:
            try:
                await self._run_session(ticker, exchange, tick_type, data_type, transform, test_mode, time_left())
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                tl = time_left()
                if tl <= 0:
                    logger.info("[%s.%s] Duration exhausted after error: %s", ticker, exchange, e)
                    return
                jitter = random.uniform(0, 0.3 * backoff)
                delay = min(backoff + jitter, max_backoff, tl)
                logger.warning("[%s.%s] Stream session error: %s; retrying in %.1fs", ticker, exchange, e, delay)
                await asyncio.sleep(delay)
                backoff = min(backoff * 2.0, max_backoff)

    def start_stream(self, command: dict):
        TEST_SERVICES = os.getenv("TEST_SERVICES", "0") == "1"
        TEST_CI = os.getenv("TEST_CI", "0") == "1"

        assert not (TEST_SERVICES and TEST_CI), "TEST_SERVICES and TEST_CI cannot both be enabled at the same time"

        test_mode = "false"
        if TEST_SERVICES:
            test_mode = "local"
        elif TEST_CI:
            test_mode = "ci"

        required_keys = ["tickers", "duration", "stream_type", "exchange"]
        for key in required_keys:
            if key not in command:
                raise ValueError(f"Missing required command key: {key}")

        ticker = command["tickers"]
        exchange = command["exchange"]
        duration = command["duration"]
        stream_type = command["stream_type"]

        if stream_type not in _TICK_TYPE_BY_STREAM_TYPE:
            raise ValueError(f"Unknown stream type: {stream_type}")

        tz_str = cast(str, ibkr_config.EXCHANGE_METADATA[exchange]["Timezone"])
        self.tz = ZoneInfo(tz_str)

        data_type = f"streaming_{stream_type}"

        if test_mode == "ci":
            self._run_ci(data_type, ticker, exchange)
            return

        _check_restart_boundary(duration)

        asyncio.run(self._stream_data(ticker, exchange, stream_type, duration, test_mode))
