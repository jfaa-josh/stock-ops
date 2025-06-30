"""Created on Sun Jun 15 17:18:28 2025

@author: JoshFody
"""

import os
from pathlib import Path

# Access secrets or other data via root .env if it exists (not in production a.k.a. docker)
if os.getenv("ENV") != "production":
    from dotenv import load_dotenv

    dotenv_path = Path(__file__).resolve().parents[3] / ".env"
    load_dotenv(dotenv_path, override=False)

# src level
SRC_ROOT = Path(__file__).resolve().parents[2]  # config.py is one level higher than controller.py

## Data Directory
DATA_DIR = SRC_ROOT / "data"
RAW_REALTIME_DIR = DATA_DIR / "raw" / "realtime"
RAW_HISTORICAL_DIR = DATA_DIR / "raw" / "historical"

DATA_API_TOKEN = os.getenv("DATA_API_TOKEN")
if not DATA_API_TOKEN:
    raise ValueError("DATA_API_TOKEN not set. Check .env or GitHub Secrets.")

### Data Stream
TRADE_URL = f"wss://ws.eodhistoricaldata.com/ws/us?api_token={DATA_API_TOKEN}"
QUOTE_URL = f"wss://ws.eodhistoricaldata.com/ws/us-quote?api_token={DATA_API_TOKEN}"
