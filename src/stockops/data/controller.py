import logging

from stockops.data.historical.base_historical_service import AbstractHistoricalService
from stockops.data.streaming.base_streaming_service import AbstractStreamingService

logger = logging.getLogger(__name__)


class Controller:
    def __init__(
        self,
        command: dict,
        streaming_service: AbstractStreamingService | None = None,
        historical_service: AbstractHistoricalService | None = None,
    ):
        self.command = command
        self.streaming_service = streaming_service
        self.historical_service = historical_service

        if bool(self.streaming_service) == bool(self.historical_service):
            raise ValueError("Exactly one of streaming_service or historical_service must be provided.")

    def __call__(self) -> None:
        logger.info("Controller: received command %r", self.command)
        try:
            if self.streaming_service:
                logger.info("Starting streaming task")
                self.streaming_service.start_stream(self.command)

            elif self.historical_service:
                logger.info("Starting historical task")
                self.historical_service.start_historical_task(self.command)

            else:
                # Defensive fallback; should never happen due to __init__ check
                raise ValueError("No service provided to execute the command.")
        except Exception as e:
            logger.exception("Controller encountered an error: %s", e)
            raise
