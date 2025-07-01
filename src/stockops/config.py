"""Created on Sun Jun 15 17:18:28 2025

@author: JoshFody
"""

import os
from pathlib import Path

# Access secrets or other data via root .env (except if in production a.k.a. Docker)
if os.getenv("ENV") != "production":
    from dotenv import load_dotenv

    dotenv_path = Path(__file__).resolve().parents[3] / ".env"
    load_dotenv(dotenv_path, override=False)

DATA_API_TOKEN = os.getenv("DATA_API_TOKEN")
if not DATA_API_TOKEN:
    raise ValueError("DATA_API_TOKEN not set. Check .env or GitHub Secrets.")

# root level
ROOT_DIR = Path(__file__).resolve().parents[3]

# src level
SRC_DIR = Path(__file__).resolve().parents[2]  # config.py is one level higher than controller.py

## Data Directory
DATA_DIR = ROOT_DIR / "data"

DATA_DB_DATESTR = "%Y-%m-%d_%H%M%S"

### Streaming
RAW_STREAMING_DIR = DATA_DIR / "raw" / "streaming"

TRADE_URL = f"wss://ws.eodhistoricaldata.com/ws/us?api_token={DATA_API_TOKEN}"
QUOTE_URL = f"wss://ws.eodhistoricaldata.com/ws/us-quote?api_token={DATA_API_TOKEN}"

### Historical
RAW_HISTORICAL_DIR = DATA_DIR / "raw" / "historical"
