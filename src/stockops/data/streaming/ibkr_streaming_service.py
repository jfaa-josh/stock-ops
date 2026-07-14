from .base_streaming_service import AbstractStreamingService


class IBKRStreamingService(AbstractStreamingService):
    def __init__(self):
        pass

    def start_stream(self, command: dict):
        raise NotImplementedError("IBKR streaming service is not yet implemented.")
