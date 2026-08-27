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

# Compose service name for the ib-gateway container.
IBKR_HOST = "ib-gateway"

# Set by run/stockops.sh's --providers flag (ibkr_paper/ibkr_live), not .env. ib-gateway itself
# requires the bare name "TRADING_MODE" — docker-compose.yml translates one to the other.
IBKR_TRADING_MODE = os.getenv("IBKR_TRADING_MODE", "paper")
if IBKR_TRADING_MODE not in ("paper", "live"):
    raise ValueError(f"Unsupported IBKR_TRADING_MODE: {IBKR_TRADING_MODE!r}; expected 'paper' or 'live'")

# 4001/4002 are loopback-only inside the ib-gateway container; 4003/4004 are the socat-relayed ports
# reachable from here.
IBKR_PORT = 4003 if IBKR_TRADING_MODE == "live" else 4004

# Static clientId ranges per connection role (stockops/ibkr/connection.py). "account" is unused until
# Section 13's account/order work begins.
IBKR_CLIENT_ID_HISTORICAL = 10
IBKR_CLIENT_ID_STREAMING = 20
IBKR_CLIENT_ID_ACCOUNT = 30

# reqMarketDataType: 1=live, 3=delayed, 4=delayed-frozen.
IBKR_MARKET_DATA_TYPE = 1

# Same top-level keys as eodhd_config.EXCHANGE_METADATA (embedded in DB filenames, must match exactly),
# plus IBKR-only `exchange`/`primary_exchange` routing fields. Defined unconditionally — ProviderConfig
# accesses this generically.
EXCHANGE_METADATA = {
    "US": {
        "Timezone": "America/New_York",
        "Currency": "USD",
        "TradingHours": {
            "Open": "09:30:00",
            "Close": "16:00:00",
            "WorkingDays": "Mon,Tue,Wed,Thu,Fri",
        },
        "exchange": "SMART",
        "primary_exchange": "NASDAQ",
    }
}
