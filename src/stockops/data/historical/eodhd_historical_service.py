import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import aiohttp

from stockops.config import eodhd_config as cfg
from stockops.config.config import RAW_HISTORICAL_DIR
from stockops.data.sql_db import SQLiteWriter
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

    async def _fetch_data(self, ws_url: str, exp_data: dict, db_filepath: Path, table_name: str):
        rmv_fields = ["timestamp", "gmtoffset"]

        def filter_fields(record: dict) -> dict:
            return {k: v for k, v in record.items() if k not in rmv_fields}

        writer = SQLiteWriter(db_filepath, table_name)

        metadata = {
            "stream_type": table_name,
            "ticker": ws_url.split("/")[-1].split("?")[0],
            "field_descriptions": json.dumps(exp_data),
            "start_time_utc": datetime.now(UTC).isoformat(),
            "ws_url": ws_url,
            "db_path": str(db_filepath),
        }
        writer.insert_metadata(metadata)

        try:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(ws_url) as response:
                        data = await response.json()
                        if isinstance(data, list):
                            for row in data:
                                writer.insert(filter_fields(row))
                        elif isinstance(data, dict):
                            writer.insert(filter_fields(data))
                        else:
                            print(f"[ERROR] Unexpected data format: {type(data)}")
                except Exception as e:
                    print(f"[{table_name}] Connection error: {e} — retrying in 5 seconds.")
                    await asyncio.sleep(5)

        except asyncio.CancelledError:
            print(f"[{table_name}] data fetch cancelled.")

        finally:
            writer.close()

    def start_historical_task(self, command: dict):
        required_keys = ["ticker", "interval", "start", "end"]
        for key in required_keys:
            if key not in command:
                raise ValueError(f"Missing required key: {key}")

        ticker = command["ticker"]
        interval = command["interval"]

        api_token = cfg.EODHD_API_TOKEN

        if interval in INTRADAY_FREQUENCIES:
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

        db_path = get_db_filepath("historical_data", "%Y-%m-%d_%H%M%S", RAW_HISTORICAL_DIR)
        task = asyncio.get_running_loop().create_task(
            self._fetch_data(url, expected_dict, db_path, f"{ticker}_{interval}")
        )
        self.tasks.append(task)

    async def wait_for_all(self):
        await asyncio.gather(*self.tasks, return_exceptions=True)
