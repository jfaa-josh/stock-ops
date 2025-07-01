"""Created on Sun Jun 15 17:07:46 2025

@author: JoshFody
"""

import asyncio

from stockops.config import QUOTE_URL, RAW_STREAMING_DIR, TRADE_URL

from .streaming.streaming_service import StreamManager


async def run_streams(duration: int = 5, tickers: list[str] | None = None):
    if tickers is None:
        tickers = ["SPY"]
    manager = StreamManager(RAW_STREAMING_DIR.__str__())
    manager.start_stream(TRADE_URL, tickers, "trades")
    await asyncio.sleep(2)
    manager.start_stream(QUOTE_URL, tickers, "quotes")

    try:
        await asyncio.sleep(duration)
    except KeyboardInterrupt:
        print("Caught Ctrl+C, shutting down...")
    finally:
        await manager.stop_all_streams()
