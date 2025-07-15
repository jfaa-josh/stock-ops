from abc import ABC, abstractmethod


class AbstractHistoricalService(ABC):
    @abstractmethod
    def start_historical_task(self, command: dict):
        pass

    @abstractmethod
    async def wait_for_all(self):
        pass
