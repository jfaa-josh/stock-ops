"""Created on Sun Jun 15 17:18:28 2025

@author: JoshFody
"""

import os
from pathlib import Path

# root level
ROOT_DIR = Path(__file__).resolve().parents[3]

# Access secrets or other data via root .env (except if in production a.k.a. Docker)
if os.getenv("ENV") != "production":
    from dotenv import load_dotenv

    dotenv_path = ROOT_DIR / ".env"
    load_dotenv(dotenv_path, override=False)

EODHD_API_TOKEN = os.getenv("EODHD_API_TOKEN")
if not EODHD_API_TOKEN:
    raise ValueError("EODHD_API_TOKEN not set. Check .env or GitHub Secrets.")

### Streaming
EODHD_TRADE_URL = f"wss://ws.eodhistoricaldata.com/ws/us?api_token={EODHD_API_TOKEN}"
EODHD_TRADE_EXPECTED_DICT = {
    "s": "ticker code",
    "p": "price",
    "c": "conditions, see trade conditions glossary for more information",
    "v": "volume, representing the number of shares traded at the corresponding time stamp",
    "dp": "dark pool true/false",
    "ms": "market status, current state of the market for the stock (“open”, “closed”, “extended hours”)",
    "t": "timestamp in milliseconds",
}

EODHD_QUOTE_URL = f"wss://ws.eodhistoricaldata.com/ws/us-quote?api_token={EODHD_API_TOKEN}"
EODHD_QUOTE_EXPECTED_DICT = {
    "s": "ticker code",
    "ap": "ask price",
    "bp": "bid price",
    "as": "ask size",
    "bs": "bid size",
    "t": "timestamp in milliseconds",
}

### Historical
EODHD_INTRADAY_HISTORICAL_EXPECTED_DICT = {
    "datetime": "UTC datetime string YYYY-MM-DD HH:MM:SS for the start of the interval",
    "open": "Opening price of the interval",
    "high": "Highest price within the interval",
    "low": "Lowest price within the interval",
    "close": "Closing price of the interval",
    "volume": "Trading volume during the interval",
}

EODHD_INTERDAY_HISTORICAL_EXPECTED_DICT = {
    "date": "UTC datetime string YYYY-MM-DD for the end of the interval",
    "open": "Opening price of the interval",
    "high": "Highest price within the interval",
    "low": "Lowest price within the interval",
    "close": "Closing price of the interval",
    "adjusted_close": "Closing price adjusted to both splits and dividends",
    "volume": "Trading volume during the interval adjusted to splits",
}
