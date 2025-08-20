import asyncio
import json
import logging
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

import websockets

from stockops.config import config, eodhd_config
from stockops.data.database.write_buffer import emit
from stockops.data.transform import TransformData
from stockops.data.utils import utcts_to_tzstr_parsed

from .base_streaming_service import AbstractStreamingService

logger = logging.getLogger(__name__)


class EODHDStreamingService(AbstractStreamingService):
    def __init__(self):
        pass

    def make_db_filepath(self, exchange: str, year_str: str, month_str: str, day_str: str) -> Path:
        filename = f"streaming_EODHD_{exchange}_{year_str}_{month_str}_{day_str}.db"

        return config.RAW_STREAMING_DIR / filename

    def write_data(self, table_name: str, exchange: str, transformed_row: dict):
        yr_str, mo_str, day_str = utcts_to_tzstr_parsed(transformed_row["timestamp_UTC_ms"], self.tz)
        db_path = self.make_db_filepath(exchange, yr_str, mo_str, day_str)

        emit({"db_path": db_path, "table": table_name, "row": transformed_row})

    async def _stream_data(
        self,
        ws_url: str,
        stream_type: str,
        exchange: str,
        symbols: list[str],
        duration: int,
    ):
        table_name = "No Ticker Set"
        transform = TransformData("EODHD", f"streaming_{stream_type}", "to_db_writer", exchange)

        assert len(symbols) == 1, (
            "Please modify eodhd_streaming_service:_stream_data code to loop over multiple tickers..."
        )

        try:
            logger.info("Connecting to %s", ws_url)
            async with websockets.connect(ws_url) as websocket:
                subscribe_msg = {"action": "subscribe", "symbols": ",".join(symbols)}
                await websocket.send(json.dumps(subscribe_msg))
                logger.info("Subscribed to %s on %s", symbols, ws_url)

                async with asyncio.timeout(duration):
                    async for message in websocket:
                        try:
                            data = json.loads(message)

                            if "status_code" in data and "message" in data:
                                logger.info("[%s] Handshake: %s", table_name, data)
                                continue

                            if "s" not in data:
                                logger.warning("Warning: No ticker in data row, ignoring...")
                                continue
                            else:
                                table_name = data["s"]
                                logger.debug("[%s] Received data: %s", table_name, data)
                                transformed_row = transform(data)

                                self.write_data(table_name, exchange, transformed_row)

                        except json.JSONDecodeError:
                            safe_message = (
                                message.decode("utf-8", errors="replace")
                                if isinstance(message, bytes)
                                else str(message)
                            )
                            logger.warning("[%s] Warning: Non-JSON message: %s", table_name, safe_message)

                        except Exception as proc_err:
                            logger.error("[%s] Error processing message: %s", table_name, proc_err, exc_info=True)

        except TimeoutError:
            # This fires when `duration` seconds have elapsed
            logger.info("Stream duration elapsed; closing connection.")

        except Exception as conn_err:
            # connection-level failures (DNS, handshake, etc.)
            logger.warning("[%s] Connection error: %s — retrying in 5 seconds.", table_name, conn_err, exc_info=True)
            await asyncio.sleep(5)

    def start_stream(self, command: dict):
        required_keys = ["tickers", "duration", "stream_type", "exchange"]
        for key in required_keys:
            if key not in command:
                raise ValueError(f"Missing required command key: {key}")

        tickers = [command["tickers"]]  # Convert to list for EODHD compatibility, future may write to multiple tickers
        exchange = command["exchange"].lower()
        duration = command["duration"]
        stream_type = command["stream_type"]

        if stream_type == "trades":
            url = f"wss://ws.eodhistoricaldata.com/ws/{exchange}?api_token={eodhd_config.EODHD_API_TOKEN}"
        elif stream_type == "quotes":
            url = f"wss://ws.eodhistoricaldata.com/ws/{exchange}-quote?api_token={eodhd_config.EODHD_API_TOKEN}"
        else:
            raise ValueError(f"Unknown stream type: {stream_type}")

        tz_str = cast(str, eodhd_config.EXCHANGE_METADATA[exchange.upper()]["Timezone"])
        self.tz = ZoneInfo(tz_str)

        asyncio.run(self._stream_data(url, stream_type, exchange.upper(), tickers, duration))
