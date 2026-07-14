from .eodhd_historical_service import EODHDHistoricalService
from .ibkr_historical_service import IBKRHistoricalService

# Later: add additional historical services here as needed


def get_historical_service(provider: str):
    if provider == "EODHD":
        return EODHDHistoricalService()
    elif provider == "IBKR":
        return IBKRHistoricalService()
    else:
        raise ValueError(f"Unknown historical provider: {provider}")
