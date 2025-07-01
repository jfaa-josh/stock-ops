"""Created on Sun Jun 15 17:07:46 2025

@author: JoshFody
"""

import asyncio

from stockops.config import (
    DATA_DB_DATESTR,
    QUOTE_EXPECTED_DICT,
    QUOTE_URL,
    RAW_STREAMING_DIR,
    TRADE_EXPECTED_DICT,
    TRADE_URL,
)

from .streaming.streaming_service import StreamManager
from .utils import get_stream_filepath


async def run_streams(duration: int = 5, tickers: list[str] | None = None):
    if tickers is None:
        tickers = ["SPY"]

    stream_manager = StreamManager()
    db_filepath = get_stream_filepath("streaming", DATA_DB_DATESTR, RAW_STREAMING_DIR)

    stream_manager.start_stream(TRADE_URL, TRADE_EXPECTED_DICT, tickers, db_filepath, "trades", duration)

    stream_manager.start_stream(QUOTE_URL, QUOTE_EXPECTED_DICT, tickers, db_filepath, "quotes", duration)

    try:
        await asyncio.sleep(duration)
    except KeyboardInterrupt:
        print("Caught Ctrl+C, shutting down...")
    finally:
        await stream_manager.stop_all_streams()
