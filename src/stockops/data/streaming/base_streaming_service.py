from abc import ABC, abstractmethod


class AbstractStreamingService(ABC):
    @abstractmethod
    def start_stream(self, command: dict):
        pass

    @abstractmethod
    async def stop_all_streams(self):
        pass
