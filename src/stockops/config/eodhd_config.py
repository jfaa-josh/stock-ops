import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# root level
ROOT_DIR = Path(__file__).resolve().parents[3]

# Access secrets or other data via root .env (except if in production a.k.a. Docker)
if os.getenv("ENV") != "production":
    from dotenv import load_dotenv

    dotenv_path = ROOT_DIR / ".env"
    load_dotenv(dotenv_path, override=False)

EODHD_API_TOKEN = os.getenv("EODHD_API_TOKEN") or None
if EODHD_API_TOKEN:
    # Exchange metadata
    EXCHANGE_METADATA = {
        "US": {
            "Timezone": "America/New_York",
            "Currency": "USD",
            "TradingHours": {
                "Open": "09:30:00",
                "Close": "16:00:00",
                "WorkingDays": "Mon,Tue,Wed,Thu,Fri",
            },
        }
    }
else:
    logger.info("EODHD_API_TOKEN not set. Check .env or GitHub Secrets if it was intended.")
