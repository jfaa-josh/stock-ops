from .eodhd_streaming_service import EODHDStreamingService

# Later: add additional streaming services here as needed


def get_streaming_service(provider: str):
    if provider == "EODHD":
        return EODHDStreamingService()
    # elif provider == "A NEW PROVIDER HERE":
    #     pass
    else:
        raise ValueError(f"Unknown streaming provider: {provider}")
