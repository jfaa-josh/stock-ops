from .eodhd_historical_service import EODHDHistoricalService
from .ibkr_historical_service import IBKRHistoricalService

# Later: add additional historical services here as needed
_HISTORICAL_SERVICES = {
    "EODHD": EODHDHistoricalService,
    "IBKR": IBKRHistoricalService,
}


def get_historical_service(provider: str):
    try:
        return _HISTORICAL_SERVICES[provider]()
    except KeyError:
        raise ValueError(f"Unknown historical provider: {provider}") from None
