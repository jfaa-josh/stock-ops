import asyncio
import logging

from stockops.data.controller import init_controller, send_command, stop_controller_from_thread
from stockops.data.historical.providers import get_historical_service
from stockops.data.streaming.providers import get_streaming_service

USE_SERVICE = "EODHD"  # Change to your desired service
MAX_STREAMS = 5  # Set your desired max concurrent streams (historical API pulls, API streams)

# -----------------------------------------------------------------------------
# LOGGING CONFIG (optional: can be removed or replaced with project-wide setup)
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# -----------------------------------------------------------------------------
# ONE-TIME INIT: Controller + Stream Service
# -----------------------------------------------------------------------------
streaming_service = get_streaming_service(USE_SERVICE)
historical_service = get_historical_service(USE_SERVICE)
init_controller(streaming_service=streaming_service, historical_service=historical_service, max_streams=MAX_STREAMS)

# -----------------------------------------------------------------------------
# MANUAL CALL BLOCKS (run one at a time, manually)
# -----------------------------------------------------------------------------
# To use interactively, you can run these lines one-by-one in a debugger or REPL.
# For example, in VSCode, highlight and run a single command block in interactive mode.

asyncio.run(
    send_command(
        {
            "type": "fetch_historical",
            "ticker": "SPY.US",
            "interval": "1m",
            "start": "2025-07-02 09:30",
            "end": "2025-07-02 16:00",
        }
    )
)

asyncio.run(
    send_command(
        {
            "type": "fetch_historical",
            "ticker": "SPY.US",
            "interval": "d",
            "start": "2025-07-02 09:30",
            "end": "2025-07-03 16:00",
        }
    )
)

asyncio.run(send_command({"type": "start_stream", "stream_type": "trades", "tickers": ["SPY"], "duration": 10}))

asyncio.run(send_command({"type": "start_stream", "stream_type": "quotes", "tickers": ["SPY"], "duration": 10}))

# --- Example 3: Shut down early (optional) ---
asyncio.run(send_command({"type": "shutdown"}))

# --- Optional: Stop from outside sync context ---
stop_controller_from_thread()

# -----------------------------------------------------------------------------
# NOTE:
# Controller auto-shuts down after all commands complete and no active streams remain.
# You do NOT need to call `shutdown` manually unless you're aborting mid-session.


### THIS NEEDS TO CHANGE USING METADATA SO I CAN RUN THIS THROUGH CONTROLLER ###
# from stockops.data.sql_db import SQLiteReader
# from stockops.config.config import RAW_STREAMING_DIR, RAW_HISTORICAL_DIR

# reader = SQLiteReader(RAW_HISTORICAL_DIR/'intraday_2025-07_EODHD.db')
# print(reader.list_tables())  # ['trades', 'quotes', 'stream_metadata']
# spy_intradata = reader.fetch_all("SPY.US")

# reader = SQLiteReader(RAW_HISTORICAL_DIR/'interday_EODHD.db')
# print(reader.list_tables())  # ['trades', 'quotes', 'stream_metadata']
# spy_interdata = reader.fetch_all("SPY.US")

# reader = SQLiteReader(RAW_STREAMING_DIR/'interday_EODHD.db')
# trades = reader.fetch_all("trades")
# quotes = reader.fetch_where("quotes", "s = ?", ["SPY"])
# metadata = reader.fetch_metadata()
# # Raw SQL
# tickers = reader.execute_raw_query("SELECT DISTINCT tickers FROM stream_metadata")
