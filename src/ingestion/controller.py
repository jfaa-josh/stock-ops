# -*- coding: utf-8 -*-
"""
Created on Sun Jun 15 17:07:46 2025

@author: JoshFody
"""
import asyncio
import sys
from typing import Optional
from streaming_service import StreamManager

API_TOKEN = '64debd8a818cb5.26335778'
TRADE_URL = f"wss://ws.eodhistoricaldata.com/ws/us?api_token={API_TOKEN}"
QUOTE_URL = f"wss://ws.eodhistoricaldata.com/ws/us-quote?api_token={API_TOKEN}"
DB_PATH = "stream_data.db"

async def run_streams(duration: Optional[int] = None):
    manager = StreamManager(DB_PATH)
    manager.start_stream(TRADE_URL, ['SPY'], 'trades')
    await asyncio.sleep(2)
    manager.start_stream(QUOTE_URL, ['SPY'], 'quotes')

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

if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(run_streams())



