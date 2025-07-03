import asyncio
import sys

from stockops.data.controller import init_controller, orchestrate, send_command
from stockops.data.historical.providers import get_historical_service
from stockops.data.streaming.providers import get_streaming_service

PROVIDER = "EODHD"

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

stream_service = get_streaming_service(PROVIDER)
hist_service = get_historical_service(PROVIDER)

init_controller(stream_service, hist_service)


async def main():
    # Start the orchestrator task (dispatcher loop)
    orchestrator_task = asyncio.create_task(orchestrate())

    # Send commands
    await send_command(
        {
            "type": "fetch_historical",
            "ticker": "SPY.US",
            "interval": "1m",
            "start": "2025-07-02 09:30",
            "end": "2025-07-02 16:00",
        }
    )

    await send_command(
        {
            "type": "fetch_historical",
            "ticker": "SPY.US",
            "interval": "d",
            "start": "2025-07-02 09:30",
            "end": "2025-07-03 16:00",
        }
    )

    # Shutdown cleanly
    await send_command({"type": "shutdown"})

    # Wait for orchestrator to clean up all background tasks
    await orchestrator_task


if __name__ == "__main__":
    asyncio.run(main())

# send({'type': 'fetch_historical', 'ticker': 'SPY.US', 'interval': 'd',
# 'start': '2025-07-02 09:30', 'end': '2025-07-02 16:00'})

# send({'type': 'start_stream', 'stream_type': 'trades', 'tickers': ['SPY'], 'duration': 10})
# send({'type': 'start_stream', 'stream_type': 'quotes', 'tickers': ['SPY'], 'duration': 10})

# send({'type': 'shutdown'})


# ### THIS NEEDS TO CHANGE USING METADATA SO I CAN RUN THIS THROUGH CONTROLLER ###
# from stockops.data.sql_db import SQLiteReader
# from pathlib import Path
# from stockops.config.config import RAW_STREAMING_DIR

# reader = SQLiteReader(RAW_STREAMING_DIR/'streaming_2025-07-02_090108.db')

# print(reader.list_tables())  # ['trades', 'quotes', 'stream_metadata']

# trades = reader.fetch_all("trades")
# quotes = reader.fetch_where("quotes", "s = ?", ["SPY"])
# metadata = reader.fetch_metadata()
# # Raw SQL
# tickers = reader.execute_raw_query("SELECT DISTINCT tickers FROM stream_metadata")
