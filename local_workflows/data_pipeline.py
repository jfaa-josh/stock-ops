import logging

from stockops.data.controller import (
    init_controller,
    send_command,
    start_controller_in_thread,
    stop_controller_from_thread,
)
from stockops.data.historical.providers import get_historical_service
from stockops.data.streaming.providers import get_streaming_service


logger = logging.getLogger("data_pipeline")

def setup_controller(streaming_provider: str = "EODHD",
                     historical_provider: str = "EODHD",
                     max_streams: int = 5) -> None:
    """
    Initialize the controller with streaming and historical services,
    then start its background thread.
    """
    stream_svc = get_streaming_service(streaming_provider)
    hist_svc = get_historical_service(historical_provider)

    logger.info(
        "Initializing controller (stream=%s, historical=%s, max_streams=%d)",
        streaming_provider,
        historical_provider,
        max_streams,
    )
    init_controller(stream_svc, hist_svc, max_streams=max_streams)

    logger.info("Starting controller thread")
    start_controller_in_thread()


async def emit_command(command: dict):
    """
    Send a single command into the controller's queue.
    """
    logger.info(f"Queuing command: {command!r}")
    await send_command(command)


def teardown_controller():
    """
    Signal the controller thread to exit.
    """
    logger.info("Stopping controller thread…")
    stop_controller_from_thread()


async def controller_driver_flow(
    commands: list[dict],
    streaming_provider: str = "EODHD",
    historical_provider: str = "EODHD",
    max_streams: int = 5
):
    # 1. init & start
    setup_controller(streaming_provider, historical_provider, max_streams)

    # 2. send your commands
    for cmd in commands:
        await emit_command(cmd)

    # 3. clean up
    teardown_controller()


if __name__ == "__main__":
    commands = [
        {"type": "start_stream", "stream_type": "trades", "tickers": ["SPY"], "duration": 10},
        # {"type": "start_stream", "stream_type": "quotes", "tickers": ["SPY"], "duration": 10},
        # {"type": "fetch_historical", "ticker": "SPY.US", "interval": "1m",
        #  "start": "2025-07-02 09:30", "end": "2025-07-02 16:00"},
        # {"type": "fetch_historical", "ticker": "SPY.US", "interval": "d",
        #  "start": "2025-07-02 09:30", "end": "2025-07-03 16:00"},
        ]

    _ = controller_driver_flow(commands, streaming_provider = "EODHD", historical_provider = "EODHD", max_streams = 5)



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
