import asyncio
import json
import logging
import os
import random
import socket
from pathlib import Path
from typing import cast
from urllib.parse import urlparse, urlunparse
from zoneinfo import ZoneInfo

import websockets

from stockops.config import config, eodhd_config
from stockops.data.database.write_buffer import emit
from stockops.data.transform import TransformData
from stockops.data.utils import get_db_filename_for_date

from .base_streaming_service import AbstractStreamingService

logger = logging.getLogger(__name__)


class EODHDStreamingService(AbstractStreamingService):
    def __init__(self):
        pass

    def write_data(self, table_name: str, exchange: str, transformed_row: dict, test_mode: str, data_type: str):
        filename = get_db_filename_for_date(
            "streaming", self.tz, "EODHD", exchange, transformed_row["timestamp_UTC_ms"]
        )
        db_path = Path(config.RAW_STREAMING_DIR) / str(filename)

        if test_mode == "false":
            emit({"db_path": db_path, "table": table_name, "row": transformed_row})
        elif test_mode == "local":
            print({"db_path": db_path, "table": table_name, "row": transformed_row})
        elif test_mode == "ci":
            if data_type == "streaming_trades":
                expected = [("timestamp_UTC_ms", int), ("price", float), ("volume", int)]

                assert len(transformed_row) == len(expected), "Length of transformed != length of expected"
                for key, expected_type in expected:
                    assert key in transformed_row, f"Missing key {key} in intraday data"
                    assert isinstance(transformed_row[key], expected_type), (
                        f"Key {key} has type {type(transformed_row[key]).__name__}, expected {expected_type.__name__}"
                    )
            elif data_type == "streaming_quotes":
                expected = [
                    ("timestamp_UTC_ms", int),
                    ("ask_price", float),
                    ("bid_price", float),
                    ("ask_size", int),
                    ("bid_size", int),
                ]

                assert len(transformed_row) == len(expected), "Length of transformed != length of expected"
                for key, expected_type in expected:
                    assert key in transformed_row, f"Missing key {key} in intraday data"
                    assert isinstance(transformed_row[key], expected_type), (
                        f"Key {key} has type {type(transformed_row[key]).__name__}, expected {expected_type.__name__}"
                    )

    async def _stream_data(
        self, ws_url: str, stream_type: str, exchange: str, tickers: list[str], duration: int, test_mode: str
    ):
        table_name = "No Ticker Set"  # This pulls from actual returned data rather than tickers
        data_type = f"streaming_{stream_type}"
        transform = TransformData("EODHD", data_type, "to_db_writer", exchange)

        assert len(tickers) == 1, (
            "Please modify eodhd_streaming_service:_stream_data code to loop over multiple tickers..."
        )

        loop = asyncio.get_running_loop()
        started = loop.time()
        backoff = 1.0
        max_backoff = 60.0
        last_ipv6_url = None

        async def run_stream(
            connect_url: str, *, server_hostname: str | None = None, host_header: str | None = None
        ) -> None:
            nonlocal backoff
            async with websockets.connect(
                connect_url,
                ping_interval=45,
                ping_timeout=45,
                close_timeout=15,
                max_queue=1000,
                max_size=None,
                server_hostname=server_hostname,
                extra_headers={"Host": host_header} if host_header else None,
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

                subscribe_msg = {"action": "subscribe", "symbols": ",".join(tickers)}
                await websocket.send(json.dumps(subscribe_msg))
                logger.info("Subscribed to [%s] on %s", ",".join(tickers), connect_url)

                tl = time_left()

                async def run_read_loop(buf_raw, buf_parsed, data_type) -> None:
                    if buf_raw is not None and isinstance(buf_parsed, dict):
                        process_parsed(buf_parsed, buf_raw, data_type)

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
                        process_parsed(data, message, data_type)

                if tl and tl > 0:
                    async with asyncio.timeout(tl):
                        await run_read_loop(buffered_raw, buffered_parsed, data_type)
                else:
                    await run_read_loop(buffered_raw, buffered_parsed, data_type)

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

        def process_parsed(data: dict, raw: str | bytes, data_type: str) -> None:
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
            self.write_data(table_name, exchange, transformed_row, test_mode, data_type)

        mock_message: str = ""
        if test_mode == "ci":
            if data_type == "streaming_trades":
                mock_message = '{"s":"SPY","p":657.5311,"v":5,"e":14,"c":[37],"dp":false,"t":1757623532850}'
            elif data_type == "streaming_quotes":
                mock_message = '{"s":"SPY","ap":657.6079,"as":5,"bp":657.5421,"bs":6,"t":1757623905553}'
            else:
                raise ValueError(f"Unsupported data_type in CI: {data_type}")
            data = json.loads(mock_message)

            process_parsed(data, mock_message, data_type)
            return  # Avoid falling into the live loop in ci mode

        while True:
            # Global duration gate
            tl = time_left()
            if tl is not None and tl <= 0:
                logger.info("Stream duration elapsed; stopping.")
                return

            try:
                logger.info("Connecting to %s", ws_url)
                await run_stream(ws_url)

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
                # If the first attempt failed, try a single IPv6 fallback (without forcing IPv6 permanently).
                if last_ipv6_url is None:
                    try:
                        parsed = urlparse(ws_url)
                        host = parsed.hostname
                        if host:
                            infos = socket.getaddrinfo(host, None, socket.AF_INET6, socket.SOCK_STREAM)
                            if infos:
                                ipv6 = infos[0][4][0]
                                netloc = f"[{ipv6}]"
                                if parsed.port:
                                    netloc += f":{parsed.port}"
                                last_ipv6_url = urlunparse(
                                    (parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
                                )
                                logger.info("Retrying with IPv6 literal: %s", last_ipv6_url)
                                await run_stream(last_ipv6_url, server_hostname=host, host_header=host)
                                continue
                    except Exception as fallback_err:
                        logger.warning("[%s] IPv6 fallback failed: %s", table_name, fallback_err)

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
        TEST_CI = os.getenv("TEST_CI", "0") == "1"

        assert not (TEST_SERVICES and TEST_CI), "TEST_SERVICES and TEST_CI cannot both be enabled at the same time"

        test_mode = "false"
        if TEST_SERVICES:
            test_mode = "local"
        elif TEST_CI:
            test_mode = "ci"

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

        asyncio.run(self._stream_data(url, stream_type, exchange.upper(), tickers, duration, test_mode))
