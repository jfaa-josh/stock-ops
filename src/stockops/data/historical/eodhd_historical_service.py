import logging
import os
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

import requests

from stockops.config import config, eodhd_config
from stockops.data.database.write_buffer import emit
from stockops.data.transform import TransformData
from stockops.data.utils import get_db_filename_for_date, tzstr_to_utcts, validate_isodatestr

from .base_historical_service import AbstractHistoricalService

INTRADAY_FREQUENCIES = {"1m", "5m", "1h"}
INTERDAY_FREQUENCIES = {"d", "w", "m"}

logger = logging.getLogger(__name__)


class EODHDHistoricalService(AbstractHistoricalService):
    def __init__(self):
        pass

    def write_data(self, data_type: str, table_name: str, exchange: str, transformed_row: dict, test_mode: str):
        transformer_data_type = f"historical_{data_type}"
        if data_type == "interday":
            entry_datetime = None
        elif data_type == "intraday":
            entry_datetime = transformed_row["timestamp_UTC_s"]

        filename = get_db_filename_for_date(transformer_data_type, self.tz, "EODHD", exchange, entry_datetime)
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
                    ("interval", str),
                ]

                assert len(transformed_row) == len(expected), "Length of transformed != length of expected"
                for key, expected_type in expected:
                    assert key in transformed_row, f"Missing key {key} in intraday data"
                    assert isinstance(transformed_row[key], expected_type), (
                        f"Key {key} has type {type(transformed_row[key]).__name__}, expected {expected_type.__name__}"
                    )
            elif data_type == "interday":
                expected = [
                    ("date", str),
                    ("open", float),
                    ("high", float),
                    ("low", float),
                    ("close", float),
                    ("adjusted_close", float),
                    ("volume", int),
                    ("interval", str),
                ]

                assert len(transformed_row) == len(expected), "Length of transformed != length of expected"
                for key, expected_type in expected:
                    assert key in transformed_row, f"Missing key {key} in intraday data"
                    assert isinstance(transformed_row[key], expected_type), (
                        f"Key {key} has type {type(transformed_row[key]).__name__}, expected {expected_type.__name__}"
                    )

    def _fetch_data(self, ws_url: str, data_type: str, exchange: str, ticker: str, interval: str, test_mode: str):
        transform = TransformData("EODHD", f"historical_{data_type}", "to_db_writer", exchange)
        table_name = ticker

        if test_mode == "ci":
            if data_type == "intraday":
                data = [
                    {
                        "timestamp": 1751463000,
                        "gmtoffset": 0,
                        "datetime": "2025-07-02 13:30:00",
                        "open": 617.23999,
                        "high": 618.71997,
                        "low": 616.609985,
                        "close": 618.599975,
                        "volume": 11824245,
                    }
                ]
            elif data_type == "interday":
                data = [
                    {
                        "date": "2024-10-25",
                        "open": 534.65,
                        "high": 537.2601,
                        "low": 531.414,
                        "close": 532.26,
                        "adjusted_close": 527.1013,
                        "volume": 4327190,
                    }
                ]
        else:
            try:
                resp = requests.get(ws_url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.warning("[%s] HTTP error fetching %s: %s", f"{ticker}.{exchange}", ws_url, e)
                return

        if isinstance(data, list):
            for row in data:
                logger.debug("[%s] Received data: %s", f"{ticker}.{exchange}", row)
                transformed_row = transform(row, interval)

                self.write_data(data_type, table_name, exchange, transformed_row, test_mode)
        elif isinstance(data, dict):
            logger.debug("[%s] Received data: %s", f"{ticker}.{exchange}", data)
            transformed_row = transform(data, interval)

            self.write_data(data_type, table_name, exchange, transformed_row, test_mode)
        else:
            logger.error("[%s] Unexpected data format: %s", f"{ticker}.{exchange}", type(data).__name__)

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
        ticker_exch = f"{ticker}.{exchange}"
        interval = command["interval"]
        api_token = eodhd_config.EODHD_API_TOKEN

        tz_str = cast(str, eodhd_config.EXCHANGE_METADATA[exchange]["Timezone"])
        self.tz = ZoneInfo(tz_str)

        if interval in INTRADAY_FREQUENCIES:
            data_type = "intraday"
            start = str(tzstr_to_utcts(command["start"], "%Y-%m-%d %H:%M", self.tz))
            end = str(tzstr_to_utcts(command["end"], "%Y-%m-%d %H:%M", self.tz))
            url = (
                f"https://eodhd.com/api/intraday/{ticker_exch}?api_token={api_token}"
                f"&interval={interval}&from={start}&to={end}&fmt=json"
            )

        elif interval in INTERDAY_FREQUENCIES:
            data_type = "interday"
            start = validate_isodatestr(command["start"])
            end = validate_isodatestr(command["end"])

            url = (
                f"https://eodhd.com/api/eod/{ticker_exch}?api_token={api_token}"
                f"&period={interval}&from={start}&to={end}&fmt=json"
            )

        else:
            raise ValueError(f"Unknown interval type: {interval}")

        logger.info("Prepared historical task: type=%s ticker=%s interval=%s", data_type, ticker_exch, interval)

        self._fetch_data(url, data_type, exchange, ticker, interval, test_mode)
