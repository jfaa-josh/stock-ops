from .eodhd_streaming_service import EODHDStreamingService
from .ibkr_streaming_service import IBKRStreamingService

# Later: add additional streaming services here as needed


def get_streaming_service(provider: str):
    if provider == "EODHD":
        return EODHDStreamingService()
    elif provider == "IBKR":
        return IBKRStreamingService()
    else:
        raise ValueError(f"Unknown streaming provider: {provider}")
