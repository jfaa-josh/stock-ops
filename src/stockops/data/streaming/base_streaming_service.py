from abc import ABC, abstractmethod


class AbstractStreamingService(ABC):
    @abstractmethod
    def start_stream(self, command: dict):
        pass
