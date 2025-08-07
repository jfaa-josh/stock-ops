"""Created on Sun Jun 15 15:35:25 2025

@author: JoshFody
"""

import asyncio
import json
import logging

import websockets

from stockops.config import eodhd_config as cfg
from stockops.data.transform import TransformData

from .base_streaming_service import AbstractStreamingService

logger = logging.getLogger(__name__)


class EODHDStreamingService(AbstractStreamingService):
    def __init__(self):
        self.tasks = []

    async def _stream_data(self, ws_url: str, exp_data: dict, stream_type: str, symbols: list[str], duration: int):
        expected_keys = set(exp_data)
        table_name = "No Ticker Set"
        transform = TransformData("EODHD", f"streaming_{stream_type}")

        assert len(symbols) == 1, (
            "Please modify eodhd_streaming_service:_stream_data code to loop over multiple tickers..."
        )

        try:
            end_time = asyncio.get_running_loop().time() + duration
            while asyncio.get_running_loop().time() < end_time:
                try:
                    logger.info("Connecting to %s", ws_url)
                    async with websockets.connect(ws_url) as websocket:
                        subscribe_msg = {"action": "subscribe", "symbols": ",".join(symbols)}
                        await websocket.send(json.dumps(subscribe_msg))
                        logger.info("Subscribed to %s on %s", symbols, ws_url)

                        async for message in websocket:
                            try:
                                data = json.loads(message)

                                if "status_code" in data and "message" in data:
                                    logger.info("[%s] Handshake: %s", table_name, data)
                                    continue

                                if expected_keys.issubset(data):
                                    table_name = data["s"]
                                    # !!! HERE IS WHERE THE DATA IS WRITTEN TO THE DB !!!
                                    logger.debug("[%s] Received data: %s", table_name, data)
                                    transformed_row = transform(data)
                                    print(transformed_row)
                                else:
                                    logger.debug("[%s] Ignored non-trade message: %s", table_name, data)

                            except json.JSONDecodeError:
                                safe_message = (
                                    message.decode("utf-8", errors="replace")
                                    if isinstance(message, bytes)
                                    else str(message)
                                )
                                logger.warning("[%s] Warning: Non-JSON message: %s", table_name, safe_message)

                            except Exception as e:
                                logger.error("[%s] Error during message processing: %s", table_name, e)

                except Exception as e:
                    logger.warning("[%s] Connection error: %s — retrying in 5 seconds.", table_name, e)
                    await asyncio.sleep(5)

        except asyncio.CancelledError:
            logger.info("[%s] Stream cancelled.", table_name)

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

        task = asyncio.get_running_loop().create_task(
            self._stream_data(url, expected_dict, stream_type, tickers, duration)
        )
        self.tasks.append(task)
        return task

    async def stop_all_streams(self):
        logger.info("Stopping all streams...")
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        logger.info("All streams stopped.")
