import asyncio
from datetime import UTC, datetime

import aiohttp

from stockops.config import eodhd_config as cfg
from stockops.config.config import RAW_HISTORICAL_DIR
from stockops.data.sql_db import WriterRegistry
from stockops.data.utils import get_db_filepath

from .base_historical_service import AbstractHistoricalService

INTRADAY_FREQUENCIES = {"1m", "5m", "1h"}
INTERDAY_FREQUENCIES = {"d", "w", "m"}


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
        print(exp_data)  # TODO: THIS IS A PLACEHOLDER OR METADATA !!!!!

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
                            print(f"[ERROR] Unexpected data format: {type(data)}")
                except Exception as e:
                    print(f"[{table_name}] Connection error: {e} — retrying in 5 seconds.")
                    await asyncio.sleep(5)

        except asyncio.CancelledError:
            print(f"[{table_name}] data fetch cancelled.")

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
            if "start" not in command or "end" not in command:
                raise ValueError("Intraday data requires 'start' and 'end' in 'YYYY-MM-DD HH:MM' format.")

            start = self._to_unix_utc(command["start"])
            end = self._to_unix_utc(command["end"])

            url = (
                f"https://eodhd.com/api/intraday/{ticker}?api_token={api_token}"
                f"&interval={interval}&from={start}&to={end}&fmt=json"
            )

            expected_dict = cfg.EODHD_INTRADAY_HISTORICAL_EXPECTED_DICT

        elif interval in INTERDAY_FREQUENCIES:
            data_type = "interday"
            if "start" not in command or "end" not in command:
                raise ValueError("Daily/monthly data requires 'start' and 'end' in 'YYYY-MM-DD HH:MM' format.")

            start = self._to_iso_date(command["start"])
            end = self._to_iso_date(command["end"])

            url = (
                f"https://eodhd.com/api/eod/{ticker}?api_token={api_token}"
                f"&period={interval}&from={start}&to={end}&fmt=json"
            )

            expected_dict = cfg.EODHD_INTERDAY_HISTORICAL_EXPECTED_DICT

        else:
            raise ValueError(f"Unknown interval type: {interval}")

        task = asyncio.get_running_loop().create_task(self._fetch_data(url, expected_dict, data_type, ticker))
        self.tasks.append(task)

    async def wait_for_all(self):
        await asyncio.gather(*self.tasks, return_exceptions=True)
