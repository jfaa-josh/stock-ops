import asyncio
import logging
from typing import Any

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

    async def __call__(self) -> None:
        logger.info("Controller: received command %r", self.command)
        try:
            typ = self.command.get("type")

            if typ == "start_stream":
                if not self.streaming_service:
                    raise ValueError("Streaming service not provided for start_stream command")
                logger.info("Starting streaming task")
                result: Any = self.streaming_service.start_stream(self.command)

            elif typ == "fetch_historical":
                if not self.historical_service:
                    raise ValueError("Historical service not provided for fetch_historical command")
                logger.info("Starting historical task")
                result = self.historical_service.start_historical_task(self.command)

            else:
                raise ValueError(f"Unknown command type: {typ}")

            if result is not None and asyncio.iscoroutine(result):
                logger.info("Awaiting asynchronous result")
                await result
                logger.info("Asynchronous result complete")

        finally:
            logger.info("Initiating shutdown procedures")

            if self.streaming_service and hasattr(self.streaming_service, "stop_all_streams"):
                logger.info("Stopping all streaming services")
                await self.streaming_service.stop_all_streams()

            if self.historical_service and hasattr(self.historical_service, "wait_for_all"):
                logger.info("Waiting for all historical tasks to complete")
                await self.historical_service.wait_for_all()

            logger.info("Shutdown procedures complete")
