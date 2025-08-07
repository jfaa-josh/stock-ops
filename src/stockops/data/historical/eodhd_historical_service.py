import asyncio
import logging
from datetime import UTC, datetime

import aiohttp

from stockops.config import eodhd_config as cfg
from stockops.data.transform import TransformData

from .base_historical_service import AbstractHistoricalService

INTRADAY_FREQUENCIES = {"1m", "5m", "1h"}
INTERDAY_FREQUENCIES = {"d", "w", "m"}

logger = logging.getLogger(__name__)


class EODHDHistoricalService(AbstractHistoricalService):
    def __init__(self):
        self.tasks = []

    def _to_unix_utc(self, dt_str: str) -> str:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        return str(int(dt.replace(tzinfo=UTC).timestamp()))

    def _to_iso_date(self, dt_str: str) -> str:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        return dt.date().isoformat()

    async def _fetch_data(self, ws_url: str, exp_data: dict, data_type: str, table_name: str):
        expected_keys = set(exp_data)
        transform = TransformData("EODHD", f"historical_{data_type}")

        try:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(ws_url) as response:
                        data = await response.json()

                        if isinstance(data, list):
                            for row in data:
                                if expected_keys.issubset(row):
                                    # !!! HERE IS WHERE THE DATA IS WRITTEN TO THE DB !!!
                                    logger.debug("[%s] Received data: %s", table_name, row)
                                    transformed_row = transform(row)
                                    print(transformed_row)
                                else:
                                    logger.debug("[%s] Ignored non-trade message: %s", table_name, row)

                        elif isinstance(data, dict):
                            if expected_keys.issubset(data):
                                # !!! HERE IS WHERE THE DATA IS WRITTEN TO THE DB !!!
                                logger.debug("[%s] Received data: %s", table_name, data)
                                transformed_row = transform(data)
                                print(transformed_row)
                            else:
                                logger.debug("[%s] Ignored non-trade message: %s", table_name, data)

                        else:
                            logger.error("[%s] Unexpected data format: %s", table_name, type(data).__name__)

                except Exception as e:
                    logger.warning("[%s] Connection error: %s — retrying in 5 seconds.", table_name, e)
                    await asyncio.sleep(5)

        except asyncio.CancelledError:
            logger.info("[%s] Data fetch cancelled.", table_name)

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
            expected_dict = cfg.EODHD_INTRADAY_HISTORICAL_EXPECTED_DICT

        elif interval in INTERDAY_FREQUENCIES:
            data_type = "interday"
            start = self._to_iso_date(command["start"])
            end = self._to_iso_date(command["end"])
            url = (
                f"https://eodhd.com/api/eod/{ticker}?api_token={api_token}"
                f"&period={interval}&from={start}&to={end}&fmt=json"
            )
            expected_dict = cfg.EODHD_INTERDAY_HISTORICAL_EXPECTED_DICT

        else:
            raise ValueError(f"Unknown interval type: {interval}")

        logger.info("Prepared historical task: type=%s ticker=%s interval=%s", data_type, ticker, interval)

        task = asyncio.get_running_loop().create_task(self._fetch_data(url, expected_dict, data_type, ticker))
        self.tasks.append(task)
        return task

    async def wait_for_all(self):
        logger.info("Awaiting completion of all historical tasks...")
        await asyncio.gather(*self.tasks, return_exceptions=True)
        logger.info("All historical tasks completed.")
