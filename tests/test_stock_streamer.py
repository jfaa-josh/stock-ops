"""Spyder Editor

This is a temporary script file.
"""

import asyncio
import sys

from stockops import run_data_pipeline

if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(run_data_pipeline(10, ["SPY"]))
