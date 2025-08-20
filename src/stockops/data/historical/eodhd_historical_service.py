import logging
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

import requests

from stockops.config import config, eodhd_config
from stockops.data.database.write_buffer import emit
from stockops.data.transform import TransformData
from stockops.data.utils import tzstr_to_utcts, utcts_to_tzstr_parsed, validate_isodatestr

from .base_historical_service import AbstractHistoricalService

INTRADAY_FREQUENCIES = {"1m", "5m", "1h"}
INTERDAY_FREQUENCIES = {"d", "w", "m"}

logger = logging.getLogger(__name__)


class EODHDHistoricalService(AbstractHistoricalService):
    def __init__(self):
        pass

    def make_db_filepath(self, data_type: str, exchange: str, year_str: str = "", month_str: str = "") -> Path:
        if data_type == "interday":
            filename = f"historical_{data_type}_EODHD_{exchange}.db"
        elif data_type == "intraday":
            filename = f"historical_{data_type}_EODHD_{exchange}_{year_str}_{month_str}.db"

        return config.RAW_HISTORICAL_DIR / filename

    def write_data(self, data_type: str, table_name: str, exchange: str, transformed_row: dict):
        if data_type == "interday":
            db_path = self.make_db_filepath(data_type, exchange)
        elif data_type == "intraday":
            yr_str, mo_str, _ = utcts_to_tzstr_parsed(transformed_row["timestamp_UTC_s"], self.tz)
            db_path = self.make_db_filepath(data_type, exchange, yr_str, mo_str)

        emit({"db_path": db_path, "table": table_name, "row": transformed_row})

    def _fetch_data(self, ws_url: str, data_type: str, exchange: str, table_name: str, interval: str):
        transform = TransformData("EODHD", f"historical_{data_type}", "to_db_writer", exchange)

        try:
            resp = requests.get(ws_url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning("[%s] HTTP error fetching %s: %s", table_name, ws_url, e)
            return

        if isinstance(data, list):
            for row in data:
                logger.debug("[%s] Received data: %s", table_name, row)
                transformed_row = transform(row, interval)

                self.write_data(data_type, table_name, exchange, transformed_row)
        elif isinstance(data, dict):
            logger.debug("[%s] Received data: %s", table_name, data)
            transformed_row = transform(data, interval)

            self.write_data(data_type, table_name, exchange, transformed_row)
        else:
            logger.error("[%s] Unexpected data format: %s", table_name, type(data).__name__)

    def start_historical_task(self, command: dict):
        required_keys = ["ticker", "exchange", "interval", "start", "end"]
        for key in required_keys:
            if key not in command:
                raise ValueError(f"Missing required key: {key}")

        exchange = command["exchange"]
        ticker = f"{command['ticker']}.{exchange}"
        interval = command["interval"]
        api_token = eodhd_config.EODHD_API_TOKEN

        tz_str = cast(str, eodhd_config.EXCHANGE_METADATA[exchange]["Timezone"])
        self.tz = ZoneInfo(tz_str)

        if interval in INTRADAY_FREQUENCIES:
            data_type = "intraday"
            start = str(tzstr_to_utcts(command["start"], "%Y-%m-%d %H:%M", self.tz))
            end = str(tzstr_to_utcts(command["end"], "%Y-%m-%d %H:%M", self.tz))
            url = (
                f"https://eodhd.com/api/intraday/{ticker}?api_token={api_token}"
                f"&interval={interval}&from={start}&to={end}&fmt=json"
            )

        elif interval in INTERDAY_FREQUENCIES:
            data_type = "interday"
            start = validate_isodatestr(command["start"])
            end = validate_isodatestr(command["end"])

            url = (
                f"https://eodhd.com/api/eod/{ticker}?api_token={api_token}"
                f"&period={interval}&from={start}&to={end}&fmt=json"
            )

        else:
            raise ValueError(f"Unknown interval type: {interval}")

        logger.info("Prepared historical task: type=%s ticker=%s interval=%s", data_type, ticker, interval)

        self._fetch_data(url, data_type, exchange, ticker, interval)
