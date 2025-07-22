import logging
import asyncio
import sys

from stockops.data.controller import (
    init_controller,
    send_command,
    start_controller_in_thread,
    stop_controller_from_thread,
)
from stockops.data.historical.providers import get_historical_service
from stockops.data.streaming.providers import get_streaming_service


# Configure logger
logging.basicConfig(
    level=logging.DEBUG,  # Required to see controller.py logs
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,  # <-- Overwrites any existing config (Python 3.8+)
)
logger = logging.getLogger("data_pipeline")


def setup_controller(
    streaming_provider: str = "EODHD",
    historical_provider: str = "EODHD",
    max_streams: int = 5,
) -> None:
    """
    Initialize the controller with streaming and historical services,
    then start its background thread.
    """
    stream_svc = get_streaming_service(streaming_provider)
    hist_svc = get_historical_service(historical_provider)

    logger.info(
        "Initializing controller (stream=%s, historical=%s, max_streams=%d)",
        streaming_provider,
        historical_provider,
        max_streams,
    )
    init_controller(stream_svc, hist_svc, max_streams=max_streams)

    logger.info("Starting controller thread")
    start_controller_in_thread()


async def emit_command(command: dict):
    """
    Send a single command into the controller's queue.
    """
    logger.info(f"Queuing command: {command!r}")
    await send_command(command)


def teardown_controller():
    """
    Signal the controller thread to exit.
    """
    logger.info("Stopping controller thread…")
    stop_controller_from_thread()


async def controller_driver_flow(
    commands: list[dict],
    streaming_provider: str = "EODHD",
    historical_provider: str = "EODHD",
    max_streams: int = 5,
):
    """
    High-level orchestration function: sets up controller, sends commands, then shuts down.
    """
    setup_controller(streaming_provider, historical_provider, max_streams)

    for cmd in commands:
        await emit_command(cmd)


if __name__ == "__main__":
    commands = [
        {"type": "start_stream", "stream_type": "trades", "tickers": ["SPY"], "duration": 10},
        # {"type": "start_stream", "stream_type": "quotes", "tickers": ["SPY"], "duration": 10},
        # {"type": "fetch_historical", "ticker": "SPY.US", "interval": "1m",
        #  "start": "2025-07-02 09:30", "end": "2025-07-02 16:00"},
    ]

    asyncio.run(controller_driver_flow(
        commands,
        streaming_provider="EODHD",
        historical_provider="EODHD",
        max_streams=5
    ))
