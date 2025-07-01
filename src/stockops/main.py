import asyncio
import sys
from pathlib import Path

from stockops import run_data_pipeline
from stockops.data.sql_db import SQLiteReader

if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    def data_pipeline(duration: int = 120, tickers: list[str] | None = None):
        asyncio.run(run_data_pipeline(duration, tickers))

    ### THIS NEEDS TO CHANGE USING METADATA SO I CAN RUN THIS THROUGH CONTROLLER ###
    def read_streaming_data(filepath: Path):
        reader = SQLiteReader(filepath)

        print(reader.list_tables())  # ['trades', 'quotes', 'stream_metadata']

        trades = reader.fetch_all("trades")
        quotes = reader.fetch_where("quotes", "s = ?", ["SPY"])
        metadata = reader.fetch_metadata()
        # Raw SQL
        tickers = reader.execute_raw_query("SELECT DISTINCT tickers FROM stream_metadata")

        return trades, quotes, metadata, tickers

    data_pipeline(120, ["SPY"])
