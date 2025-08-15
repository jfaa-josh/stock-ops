import logging
from datetime import date, datetime
from typing import cast
from zoneinfo import ZoneInfo

import requests

from stockops.config import eodhd_config as cfg
from stockops.data.transform import TransformData

from .base_historical_service import AbstractHistoricalService

INTRADAY_FREQUENCIES = {"1m", "5m", "1h"}
INTERDAY_FREQUENCIES = {"d", "w", "m"}

logger = logging.getLogger(__name__)


class EODHDHistoricalService(AbstractHistoricalService):
    def __init__(self):
        pass

    def localtz_to_utctimestamp(self, ts_str: str, tz: ZoneInfo):
        return int(datetime.strptime(ts_str, "%Y-%m-%d %H:%M").replace(tzinfo=tz).timestamp())

    def validate_interday(self, s: str):
        """
        Parse a date in “YYYY-MM-DD” form and return a date object.
        Raises ValueError if the format is wrong or the date is invalid.
        """
        date.fromisoformat(s)
        return s

    def _fetch_data(self, ws_url: str, data_type: str, exchange: str, table_name: str):
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
                # !!! HERE IS WHERE THE DATA IS WRITTEN TO THE DB !!!
                logger.debug("[%s] Received data: %s", table_name, row)
                transformed_row = transform(row)
                print(transformed_row)

        elif isinstance(data, dict):
            # !!! HERE IS WHERE THE DATA IS WRITTEN TO THE DB !!!
            logger.debug("[%s] Received data: %s", table_name, data)
            transformed_row = transform(data)
            print(transformed_row)

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
        api_token = cfg.EODHD_API_TOKEN

        tz_str = cast(str, cfg.EXCHANGE_METADATA[exchange]["Timezone"])
        tz = ZoneInfo(tz_str)

        if interval in INTRADAY_FREQUENCIES:
            data_type = "intraday"
            start = self.localtz_to_utctimestamp(command["start"], tz)
            end = self.localtz_to_utctimestamp(command["end"], tz)
            url = (
                f"https://eodhd.com/api/intraday/{ticker}?api_token={api_token}"
                f"&interval={interval}&from={start}&to={end}&fmt=json"
            )

        elif interval in INTERDAY_FREQUENCIES:
            data_type = "interday"
            start = self.validate_interday(command["start"])
            end = self.validate_interday(command["end"])

            url = (
                f"https://eodhd.com/api/eod/{ticker}?api_token={api_token}"
                f"&period={interval}&from={start}&to={end}&fmt=json"
            )

        else:
            raise ValueError(f"Unknown interval type: {interval}")

        logger.info("Prepared historical task: type=%s ticker=%s interval=%s", data_type, ticker, interval)

        self._fetch_data(url, data_type, exchange, ticker)
