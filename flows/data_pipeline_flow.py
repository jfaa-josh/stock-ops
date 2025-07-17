# prefect_controller_flow.py

import logging
from typing import Any, Dict, List

from prefect import flow, task, get_run_logger

from stockops.data.controller import (
    init_controller,
    send_command,
    start_controller_in_thread,
    stop_controller_from_thread,
)
from stockops.data.historical.providers import get_historical_service
from stockops.data.streaming.providers import get_streaming_service


@task
def setup_controller(
    streaming_provider: str = "EODHD",
    historical_provider: str = "EODHD",
    max_streams: int = 5,
) -> None:
    """
    Initialize the controller with streaming and historical services,
    then start its background thread.
    """
    logger = get_run_logger()
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


@task
async def emit_command(command: Dict[str, Any]) -> None:
    """
    Send a single command into the controller's queue.
    """
    logger = get_run_logger()
    logger.info("Queuing command: %r", command)
    await send_command(command)


@task
def teardown_controller() -> None:
    """
    Signal the controller thread to exit.
    """
    logger = get_run_logger()
    logger.info("Stopping controller thread")
    stop_controller_from_thread()


@flow(name="controller_driver")
def controller_driver_flow(
    commands: List[Dict[str, Any]],
    streaming_provider: str = "EODHD",
    historical_provider: str = "EODHD",
    max_streams: int = 5,
) -> None:
    """
    Prefect flow that drives the stockops controller: initialize it,
    emit a series of commands, and then cleanly shut it down.
    """
    setup_controller(streaming_provider, historical_provider, max_streams)

    try:
        for cmd in commands:
            _ = emit_command(cmd)
    finally:
        teardown_controller()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    commands_list = [
        {"type": "start_stream", "stream_type": "trades", "tickers": ["SPY"], "duration": 10},
        {"type": "start_stream", "stream_type": "quotes", "tickers": ["SPY"], "duration": 10},
        {
            "type": "fetch_historical",
            "ticker": "SPY.US",
            "interval": "1m",
            "start": "2025-07-02 09:30",
            "end": "2025-07-02 16:00",
        },
        {
            "type": "fetch_historical",
            "ticker": "SPY.US",
            "interval": "d",
            "start": "2025-07-02 09:30",
            "end": "2025-07-03 16:00",
        },
    ]

    controller_driver_flow(commands_list, streaming_provider="EODHD", historical_provider="EODHD", max_streams=5)
