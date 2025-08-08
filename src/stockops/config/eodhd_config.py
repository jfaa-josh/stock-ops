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
EODHD_QUOTE_URL = f"wss://ws.eodhistoricaldata.com/ws/us-quote?api_token={EODHD_API_TOKEN}"
