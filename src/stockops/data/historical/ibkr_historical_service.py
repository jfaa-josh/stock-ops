from .base_historical_service import AbstractHistoricalService


class IBKRHistoricalService(AbstractHistoricalService):
    def __init__(self):
        pass

    def start_historical_task(self, command: dict):
        raise NotImplementedError("IBKR historical service is not yet implemented.")
