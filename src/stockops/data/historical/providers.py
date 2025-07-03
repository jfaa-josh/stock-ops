from .eodhd_historical_service import EODHDHistoricalService

# Later: add additional historical services here as needed


def get_historical_service(provider: str):
    if provider == "EODHD":
        return EODHDHistoricalService()
    # elif provider == "A NEW PROVIDER HERE":
    #     pass
    else:
        raise ValueError(f"Unknown historical provider: {provider}")
