"""Created on Sun Jun 15 15:35:25 2025

@author: JoshFody
"""

import asyncio
import json
from datetime import UTC, datetime

import websockets

from stockops.config import eodhd_config as cfg
from stockops.config.config import RAW_STREAMING_DIR
from stockops.data.sql_db import WriterRegistry
from stockops.data.utils import get_db_filepath

from .base_streaming_service import AbstractStreamingService


class EODHDStreamingService(AbstractStreamingService):
    def __init__(self):
        self.tasks: list[asyncio.Task] = []

    async def _stream_data(self, ws_url: str, exp_data: dict, symbols: list[str], duration: int):
        expected_keys = set(exp_data)
        table_name = "No Ticker Set"

        writer_cache = {}
        try:
            end_time = asyncio.get_event_loop().time() + duration
            while asyncio.get_event_loop().time() < end_time:
                try:
                    print(f"Connecting to {ws_url}")
                    async with websockets.connect(ws_url) as websocket:
                        subscribe_msg = {"action": "subscribe", "symbols": ",".join(symbols)}
                        await websocket.send(json.dumps(subscribe_msg))
                        print(f"Subscribed to {symbols} on {ws_url}")

                        async for message in websocket:
                            try:
                                data = json.loads(message)
                                table_name = data["s"]

                                if "status_code" in data and "message" in data:
                                    print(f"[{table_name}] Handshake: {data}")
                                    continue

                                if expected_keys.issubset(data):
                                    ts = datetime.fromtimestamp(data["t"] / 1000, UTC)

                                    # Dynamically compute db_path and writer
                                    db_path = get_db_filepath("streaming", "EODHD", ts, RAW_STREAMING_DIR)
                                    writer_key = (db_path, table_name)

                                    if writer_key not in writer_cache:
                                        writer_cache[writer_key] = WriterRegistry.get_writer(db_path, table_name)

                                    await writer_cache[writer_key].write(data)
                                    print(f"[{table_name}] {data}")
                                else:
                                    print(f"[{table_name}] Ignored non-trade message: {data}")

                            except json.JSONDecodeError:
                                safe_message = (
                                    message.decode("utf-8", errors="replace")
                                    if isinstance(message, bytes)
                                    else str(message)
                                )
                                print(f"[{table_name}] Warning: Non-JSON message: {safe_message}")

                            except Exception as e:
                                print(f"[{table_name}] Error: {e}")

                except Exception as e:
                    print(f"[{table_name}] Connection error: {e} — retrying in 5 seconds.")
                    await asyncio.sleep(5)

        except asyncio.CancelledError:
            print(f"[{table_name}] Stream cancelled.")

    def start_stream(self, command: dict):
        required_keys = ["tickers", "duration", "stream_type"]
        for key in required_keys:
            if key not in command:
                raise ValueError(f"Missing required command key: {key}")

        tickers = command["tickers"]
        duration = command["duration"]
        stream_type = command["stream_type"]

        if stream_type == "trades":
            url = cfg.EODHD_TRADE_URL
            expected_dict = cfg.EODHD_TRADE_EXPECTED_DICT
        elif stream_type == "quotes":
            url = cfg.EODHD_QUOTE_URL
            expected_dict = cfg.EODHD_QUOTE_EXPECTED_DICT
        else:
            raise ValueError(f"Unknown stream type: {stream_type}")

        task = asyncio.get_running_loop().create_task(self._stream_data(url, expected_dict, tickers, duration))
        self.tasks.append(task)

    async def stop_all_streams(self):
        print("Stopping all streams...")
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        print("All streams stopped.")
