import logging
from datetime import UTC, datetime

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

    def _to_unix_utc(self, dt_str: str) -> str:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        return str(int(dt.replace(tzinfo=UTC).timestamp()))

    def _to_iso_date(self, dt_str: str) -> str:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        return dt.date().isoformat()

    def _fetch_data(self, ws_url: str, data_type: str, table_name: str):
        transform = TransformData("EODHD", f"historical_{data_type}")

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
        required_keys = ["ticker", "interval", "start", "end"]
        for key in required_keys:
            if key not in command:
                raise ValueError(f"Missing required key: {key}")

        ticker = command["ticker"]
        interval = command["interval"]
        api_token = cfg.EODHD_API_TOKEN

        if interval in INTRADAY_FREQUENCIES:
            data_type = "intraday"
            start = self._to_unix_utc(command["start"])
            end = self._to_unix_utc(command["end"])
            url = (
                f"https://eodhd.com/api/intraday/{ticker}?api_token={api_token}"
                f"&interval={interval}&from={start}&to={end}&fmt=json"
            )

        elif interval in INTERDAY_FREQUENCIES:
            data_type = "interday"
            start = self._to_iso_date(command["start"])
            end = self._to_iso_date(command["end"])
            url = (
                f"https://eodhd.com/api/eod/{ticker}?api_token={api_token}"
                f"&period={interval}&from={start}&to={end}&fmt=json"
            )

        else:
            raise ValueError(f"Unknown interval type: {interval}")

        logger.info("Prepared historical task: type=%s ticker=%s interval=%s", data_type, ticker, interval)

        self._fetch_data(url, data_type, ticker)
