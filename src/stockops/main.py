import asyncio
import sys

from stockops import run_data_pipeline

if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    def data_pipeline(duration: int = 120, tickers: list[str] | None = None):
        asyncio.run(run_data_pipeline(duration, tickers))

    # data_pipeline(120, ["SPY"])
