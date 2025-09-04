import asyncio
import json
import logging
import os
import random
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

import websockets

from stockops.config import config, eodhd_config
from stockops.data.database.write_buffer import emit
from stockops.data.transform import TransformData
from stockops.data.utils import get_db_filename_for_date

from .base_streaming_service import AbstractStreamingService

logger = logging.getLogger(__name__)

TEST_SERVICES = os.getenv("TEST_SERVICES", "0") == "1"


class EODHDStreamingService(AbstractStreamingService):
    def __init__(self):
        pass

    def write_data(self, table_name: str, exchange: str, transformed_row: dict, test_mode: bool):
        filename = get_db_filename_for_date(
            "streaming", self.tz, "EODHD", exchange, transformed_row["timestamp_UTC_ms"]
        )
        db_path = Path(config.RAW_STREAMING_DIR) / str(filename)

        if test_mode:
            print({"db_path": db_path, "table": table_name, "row": transformed_row})
        else:
            emit({"db_path": db_path, "table": table_name, "row": transformed_row})

    async def _stream_data(
        self, ws_url: str, stream_type: str, exchange: str, tickers: list[str], duration: int, test_mode: bool
    ):
        table_name = "No Ticker Set"  # This pulls from actual returned data rather than tickers
        transform = TransformData("EODHD", f"streaming_{stream_type}", "to_db_writer", exchange)

        assert len(tickers) == 1, (
            "Please modify eodhd_streaming_service:_stream_data code to loop over multiple tickers..."
        )

        loop = asyncio.get_running_loop()
        started = loop.time()
        backoff = 1.0
        max_backoff = 60.0

        def time_left() -> float | None:
            if duration is None:
                return None
            return max(0.0, duration - (loop.time() - started))

        async def maybe_retry(e: Exception, context: str) -> bool:
            """Returns True if we should retry, False if we should stop (duration exhausted)."""
            nonlocal backoff
            tl = time_left()
            if tl is not None and tl <= 0:
                logger.info("Duration exhausted after %s; stopping. Error: %s", context, e)
                return False
            jitter = random.uniform(0, 0.3 * backoff)
            delay = min(backoff + jitter, max_backoff)
            logger.warning("[%s] %s; retrying in %.1fs", table_name, context, delay)
            # If duration is bounded, don't sleep past it
            if tl is not None:
                delay = min(delay, tl)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2.0, max_backoff)
            return True

        def process_parsed(data: dict, raw: str | bytes) -> None:
            """Process one already-parsed JSON dict."""
            nonlocal table_name
            if "status_code" in data and "message" in data:
                logger.info("[%s] Handshake: %s", table_name, data)
                return
            s = data.get("s")
            if not s:
                safe = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes | bytearray)) else str(raw)
                logger.warning("[%s] Missing 's' in data; ignoring: %s", table_name, safe)
                return
            table_name = s
            logger.debug("[%s] Received: %s", table_name, data)
            transformed_row = transform(data)
            self.write_data(table_name, exchange, transformed_row, test_mode)

        while True:
            # Global duration gate
            tl = time_left()
            if tl is not None and tl <= 0:
                logger.info("Stream duration elapsed; stopping.")
                return

            try:
                logger.info("Connecting to %s", ws_url)
                async with websockets.connect(
                    ws_url,
                    ping_interval=45,
                    ping_timeout=45,
                    close_timeout=15,
                    max_queue=1000,
                    max_size=None,
                ) as websocket:
                    # Successful connect → reset backoff
                    backoff = 1.0

                    # Wait (briefly) for an auth banner; if first frame is not a banner, buffer it
                    buffered_raw = None
                    buffered_parsed = None
                    try:
                        raw0 = await asyncio.wait_for(websocket.recv(), timeout=3)
                        try:
                            parsed0 = json.loads(raw0)
                        except json.JSONDecodeError:
                            parsed0 = None
                        if isinstance(parsed0, dict) and parsed0.get("status_code") == 200:
                            logger.info("[%s] Auth banner: %s", table_name, parsed0)
                        else:
                            buffered_raw, buffered_parsed = raw0, parsed0
                            logger.debug("[%s] First frame not banner; buffering initial frame.", table_name)
                    except TimeoutError:
                        logger.debug("No auth banner within 3s; proceeding to subscribe.")

                    # Subscribe after handshake stage (or timeout)
                    subscribe_msg = {"action": "subscribe", "symbols": ",".join(tickers)}
                    await websocket.send(json.dumps(subscribe_msg))
                    logger.info("Subscribed to [%s] on %s", ",".join(tickers), ws_url)

                    # Compute remaining after subscribe
                    tl = time_left()

                    async def run_read_loop(buf_raw, buf_parsed) -> None:
                        # Process buffered frame (if any) first
                        if buf_raw is not None and isinstance(buf_parsed, dict):
                            process_parsed(buf_parsed, buf_raw)

                        # Main stream loop
                        async for message in websocket:
                            try:
                                data = json.loads(message)
                            except json.JSONDecodeError:
                                safe = (
                                    message.decode("utf-8", errors="replace")
                                    if isinstance(message, (bytes | bytearray))
                                    else str(message)
                                )
                                logger.warning("[%s] Non-JSON message: %s", table_name, safe)
                                continue
                            process_parsed(data, message)

                    if tl and tl > 0:
                        # Only apply timeout if we actually have a time budget
                        async with asyncio.timeout(tl):
                            await run_read_loop(buffered_raw, buffered_parsed)
                    else:
                        await run_read_loop(buffered_raw, buffered_parsed)

                    # If we exit the read loop normally, loop back to reconnect unless duration expired
                    continue

            except TimeoutError:
                # This comes from asyncio.timeout(tl) expiring
                logger.info("Read window expired.")
                return

            except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
                if not await maybe_retry(e, "WebSocket closed"):
                    return
                continue

            except (OSError, websockets.InvalidURI, websockets.InvalidHandshake) as e:
                if not await maybe_retry(e, "Connection error"):
                    return
                continue

            except asyncio.CancelledError:
                logger.info("[%s] Cancellation received; shutting down.", table_name)
                raise

            except Exception as e:
                if not await maybe_retry(e, "Unexpected error"):
                    return
                continue

    def start_stream(self, command: dict):
        TEST_SERVICES = os.getenv("TEST_SERVICES", "0") == "1"

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

        asyncio.run(self._stream_data(url, stream_type, exchange.upper(), tickers, duration, TEST_SERVICES))
