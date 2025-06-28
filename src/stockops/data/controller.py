"""Created on Sun Jun 15 17:07:46 2025

@author: JoshFody
"""

import asyncio

from stockops.config import DATA_DIR, QUOTE_URL, TRADE_URL

from .streaming.streaming_service import StreamManager

DB_PATH = DATA_DIR / "stream_data.db"


async def run_streams(duration: int | None = None):
    manager = StreamManager(DB_PATH.__str__())
    manager.start_stream(TRADE_URL, ["SPY"], "trades")
    await asyncio.sleep(2)
    manager.start_stream(QUOTE_URL, ["SPY"], "quotes")

    try:
        if duration is not None:
            await asyncio.sleep(duration)
        else:
            while True:
                await asyncio.sleep(3600)
    except KeyboardInterrupt:
        print("Caught Ctrl+C, shutting down...")
    finally:
        await manager.stop_all_streams()
