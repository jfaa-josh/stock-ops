from .eodhd_streaming_service import EODHDStreamingService
from .ibkr_streaming_service import IBKRStreamingService

# Later: add additional streaming services here as needed
_STREAMING_SERVICES = {
    "EODHD": EODHDStreamingService,
    "IBKR": IBKRStreamingService,
}


def get_streaming_service(provider: str):
    try:
        return _STREAMING_SERVICES[provider]()
    except KeyError:
        raise ValueError(f"Unknown streaming provider: {provider}") from None
