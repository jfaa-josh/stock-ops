"""Created on Sun Jun 15 17:18:28 2025

@author: JoshFody
"""

import os
from pathlib import Path

# root level
ROOT_DIR = Path(__file__).resolve().parents[2]

# Access secrets or other data via root .env (except if in production a.k.a. Docker)
if os.getenv("ENV") != "production":
    from dotenv import load_dotenv

    dotenv_path = ROOT_DIR / ".env"
    load_dotenv(dotenv_path, override=False)

DATA_API_TOKEN = os.getenv("DATA_API_TOKEN")
if not DATA_API_TOKEN:
    raise ValueError("DATA_API_TOKEN not set. Check .env or GitHub Secrets.")

# src level
SRC_DIR = ROOT_DIR / "src"

# stockops level
PKG_DIR = SRC_DIR / "stockops"

## Data Directory
DATA_DIR = ROOT_DIR / "data"

DATA_DB_DATESTR = "%Y-%m-%d_%H%M%S"

### Streaming
RAW_STREAMING_DIR = DATA_DIR / "raw" / "streaming"

TRADE_URL = f"wss://ws.eodhistoricaldata.com/ws/us?api_token={DATA_API_TOKEN}"
TRADE_EXPECTED_DICT = {
    "s": "ticker code",
    "p": "price",
    "c": "conditions, see trade conditions glossary for more information",
    "v": "volume, representing the number of shares traded at the corresponding time stamp",
    "dp": "dark pool true/false",
    "ms": "market status, current state of the market for the stock (“open”, “closed”, “extended hours”)",
    "t": "timestamp in milliseconds",
}

QUOTE_URL = f"wss://ws.eodhistoricaldata.com/ws/us-quote?api_token={DATA_API_TOKEN}"
QUOTE_EXPECTED_DICT = {
    "s": "ticker code",
    "ap": "ask price",
    "bp": "bid price",
    "as": "ask size",
    "bs": "bid size",
    "t": "timestamp in milliseconds",
}

### Historical
RAW_HISTORICAL_DIR = DATA_DIR / "raw" / "historical"
