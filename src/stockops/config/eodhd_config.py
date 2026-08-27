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
_EODHD_TOKEN_PLACEHOLDER = "__PUT_YOUR_TOKEN_HERE__"  # must match .env.example
if not EODHD_API_TOKEN:
    logger.info("EODHD_API_TOKEN not set. Check .env or GitHub Secrets if it was intended.")
elif EODHD_API_TOKEN == _EODHD_TOKEN_PLACEHOLDER:
    # Warning, not an exception: this module must stay importable with no real token (writer-service,
    # streamlit only need EXCHANGE_METADATA). run/stockops.sh does the hard fail-fast check instead.
    logger.warning("EODHD_API_TOKEN is still the .env.example placeholder, not a real token.")

# Defined unconditionally so ProviderConfig can always access it, even with no token set.
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
