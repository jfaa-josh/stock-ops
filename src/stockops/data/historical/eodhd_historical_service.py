import asyncio
import logging
from datetime import UTC, datetime

import aiohttp

from stockops.config import eodhd_config as cfg
from stockops.config.config import RAW_HISTORICAL_DIR
from stockops.data.sql_db import WriterRegistry
from stockops.data.utils import get_db_filepath

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
        rmv_fields = ["timestamp", "gmtoffset"]
        logger.debug("Expected schema: %s", exp_data)  # TODO: PLACEHOLDER FOR Metadata / schema debug aid

        def filter_fields(record: dict) -> dict:
            return {k: v for k, v in record.items() if k not in rmv_fields}

        writer_cache = {}
        ts = None
        try:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(ws_url) as response:
                        data = await response.json()

                        if isinstance(data, list):
                            for row in data:
                                if data_type == "intraday":
                                    ts = datetime.fromtimestamp(int(row["timestamp"]))
                                db_path = get_db_filepath(data_type, "EODHD", ts, RAW_HISTORICAL_DIR)
                                writer_key = (db_path, table_name)

                                if writer_key not in writer_cache:
                                    writer_cache[writer_key] = WriterRegistry.get_writer(db_path, table_name)

                                await writer_cache[writer_key].write(filter_fields(row))

                        elif isinstance(data, dict):
                            if data_type == "intraday":
                                ts = datetime.fromtimestamp(int(data["timestamp"]))
                            db_path = get_db_filepath(data_type, "EODHD", ts, RAW_HISTORICAL_DIR)
                            writer_key = (db_path, table_name)

                            if writer_key not in writer_cache:
                                writer_cache[writer_key] = WriterRegistry.get_writer(db_path, table_name)

                            await writer_cache[writer_key].write(filter_fields(data))

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
