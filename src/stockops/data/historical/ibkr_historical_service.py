import asyncio
import logging
import os
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

from stockops.config import config, ibkr_config
from stockops.data.database.write_buffer import emit
from stockops.data.ibkr import bars, pacing
from stockops.data.transform import TransformData
from stockops.data.utils import get_db_filename_for_date
from stockops.ibkr import contracts
from stockops.ibkr.connection import ibkr_session

from .base_historical_service import AbstractHistoricalService

INTRADAY_FREQUENCIES = {"1m", "5m", "1h"}
INTERDAY_FREQUENCIES = {"d", "w", "m"}

logger = logging.getLogger(__name__)


def _command_to_utc_datetime(value: str, data_type: str, tz: ZoneInfo) -> datetime:
    if data_type == "intraday":
        return datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=tz).astimezone(UTC)
    return datetime.combine(date.fromisoformat(value), time.min, tzinfo=UTC)


class IBKRHistoricalService(AbstractHistoricalService):
    def __init__(self):
        pass

    def write_data(self, data_type: str, table_name: str, exchange: str, transformed_row: dict, test_mode: str):
        transformer_data_type = f"historical_{data_type}"
        if data_type == "interday":
            entry_datetime = None
        elif data_type == "intraday":
            entry_datetime = transformed_row["timestamp_UTC_s"]

        filename = get_db_filename_for_date(transformer_data_type, self.tz, "IBKR", exchange, entry_datetime)
        db_path = Path(config.RAW_HISTORICAL_DIR) / str(filename)

        if test_mode == "false":
            emit({"db_path": db_path, "table": table_name, "row": transformed_row})
        elif test_mode == "local":
            print({"db_path": db_path, "table": table_name, "row": transformed_row})
        elif test_mode == "ci":
            if data_type == "intraday":
                expected = [
                    ("timestamp_UTC_s", int),
                    ("open", float),
                    ("high", float),
                    ("low", float),
                    ("close", float),
                    ("volume", int),
                    ("average", float),
                    ("barCount", int),
                    ("interval", str),
                ]
            elif data_type == "interday":
                expected = [
                    ("date", str),
                    ("open", float),
                    ("high", float),
                    ("low", float),
                    ("close", float),
                    ("adjusted_close", float),
                    ("volume", int),
                    ("average", float),
                    ("barCount", int),
                    ("interval", str),
                ]

            assert len(transformed_row) == len(expected), "Length of transformed != length of expected"
            for key, expected_type in expected:
                assert key in transformed_row, f"Missing key {key} in {data_type} data"
                assert isinstance(transformed_row[key], expected_type), (
                    f"Key {key} has type {type(transformed_row[key]).__name__}, expected {expected_type.__name__}"
                )

    def _run_ci(self, data_type: str, ticker: str, exchange: str, interval: str) -> None:
        transform = TransformData("IBKR", f"historical_{data_type}", "to_db_writer", exchange)

        if data_type == "intraday":
            data_row = {
                "date": datetime(2025, 7, 2, 13, 30, tzinfo=UTC),
                "open": 617.23999,
                "high": 618.71997,
                "low": 616.609985,
                "close": 618.599975,
                "volume": 11824245.0,
                "average": 617.9,
                "barCount": 512,
            }
        elif data_type == "interday":
            data_row = {
                "date": date(2024, 10, 25),
                "open": 534.65,
                "high": 537.2601,
                "low": 531.414,
                "close": 532.26,
                "adjusted_close": 527.1013,
                "volume": 4327190.0,
                "average": 533.9,
                "barCount": 8213,
            }
        else:
            raise ValueError(f"Unknown data_type for CI mock: {data_type}")

        logger.debug("[%s.%s] TEST_CI mock data: %s", ticker, exchange, data_row)
        transformed_row = transform(data_row, interval)
        self.write_data(data_type, ticker, exchange, transformed_row, "ci")

    async def _fetch_and_write(
        self,
        ticker: str,
        exchange: str,
        interval: str,
        data_type: str,
        bar_size: str,
        start_dt: datetime,
        end_dt: datetime,
        test_mode: str,
    ) -> None:
        transform = TransformData("IBKR", f"historical_{data_type}", "to_db_writer", exchange)
        windows = bars.generate_windows(start_dt, end_dt, bar_size)
        use_rth = ibkr_config.IBKR_USE_RTH_INTERDAY if data_type == "interday" else ibkr_config.IBKR_USE_RTH_INTRADAY
        governor = pacing.PacingGovernor()

        async with ibkr_session("historical") as ib:
            contract = await contracts.qualify_stock(ib, ticker, exchange)

            for window_num, (end_date_time, duration_str) in enumerate(windows, start=1):
                logger.info(
                    "[%s.%s] Requesting window %d/%d: end=%s duration=%s",
                    ticker,
                    exchange,
                    window_num,
                    len(windows),
                    end_date_time,
                    duration_str,
                )

                trades_sig = pacing.RequestSignature(
                    contract.conId, exchange, "TRADES", (bar_size, duration_str, end_date_time)
                )
                async with governor.acquire(trades_sig):
                    trades_bars = await ib.reqHistoricalDataAsync(
                        contract, end_date_time, duration_str, bar_size, "TRADES", use_rth, formatDate=2
                    )

                adjusted_by_date: dict = {}
                if data_type == "interday":
                    adj_sig = pacing.RequestSignature(
                        contract.conId, exchange, "ADJUSTED_LAST", (bar_size, duration_str, end_date_time)
                    )
                    async with governor.acquire(adj_sig):
                        adjusted_bars = await ib.reqHistoricalDataAsync(
                            contract, end_date_time, duration_str, bar_size, "ADJUSTED_LAST", use_rth, formatDate=2
                        )
                    adjusted_by_date = {bar.date: bar.close for bar in adjusted_bars}

                for bar in trades_bars:
                    data_row = {
                        "date": bar.date,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "average": bar.average,
                        "barCount": bar.barCount,
                    }
                    if data_type == "interday":
                        if bar.date not in adjusted_by_date:
                            raise RuntimeError(
                                f"IBKR ADJUSTED_LAST response missing a bar for date={bar.date!r} "
                                f"(ticker={ticker!r} exchange={exchange!r}); TRADES/ADJUSTED_LAST diverged."
                            )
                        data_row["adjusted_close"] = adjusted_by_date[bar.date]

                    transformed_row = transform(data_row, interval)
                    self.write_data(data_type, ticker, exchange, transformed_row, test_mode)

    def start_historical_task(self, command: dict):
        TEST_SERVICES = os.getenv("TEST_SERVICES", "0") == "1"
        TEST_CI = os.getenv("TEST_CI", "0") == "1"

        assert not (TEST_SERVICES and TEST_CI), "TEST_SERVICES and TEST_CI cannot both be enabled at the same time"

        test_mode = "false"
        if TEST_SERVICES:
            test_mode = "local"
        elif TEST_CI:
            test_mode = "ci"

        required_keys = ["ticker", "exchange", "interval", "start", "end"]
        for key in required_keys:
            if key not in command:
                raise ValueError(f"Missing required key: {key}")

        exchange = command["exchange"]
        ticker = command["ticker"]
        interval = command["interval"]

        tz_str = cast(str, ibkr_config.EXCHANGE_METADATA[exchange]["Timezone"])
        self.tz = ZoneInfo(tz_str)

        if interval in INTRADAY_FREQUENCIES:
            data_type = "intraday"
        elif interval in INTERDAY_FREQUENCIES:
            data_type = "interday"
        else:
            raise ValueError(f"Unknown interval type: {interval}")

        logger.info("Prepared historical task: type=%s ticker=%s.%s interval=%s", data_type, ticker, exchange, interval)

        if test_mode == "ci":
            self._run_ci(data_type, ticker, exchange, interval)
            return

        bar_size = bars.BAR_SIZE_SETTINGS[interval]
        start_dt = _command_to_utc_datetime(command["start"], data_type, self.tz)
        end_dt = _command_to_utc_datetime(command["end"], data_type, self.tz)

        asyncio.run(self._fetch_and_write(ticker, exchange, interval, data_type, bar_size, start_dt, end_dt, test_mode))
