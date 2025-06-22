"""Created on Sun Jun 15 17:18:28 2025

@author: JoshFody
"""

from pathlib import Path

# Repo level
REPO_ROOT = Path(__file__).resolve().parents[2]  # config.py is one level higher than controller.py
DATA_DIR = REPO_ROOT / "data"
RAW_REALTIME_DIR = DATA_DIR / "raw" / "realtime"

# src level

## Data Directory
API_TOKEN = "64debd8a818cb5.26335778"

### Data Stream
TRADE_URL = f"wss://ws.eodhistoricaldata.com/ws/us?api_token={API_TOKEN}"
QUOTE_URL = f"wss://ws.eodhistoricaldata.com/ws/us-quote?api_token={API_TOKEN}"
