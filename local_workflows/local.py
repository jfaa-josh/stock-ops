import logging
from typing import Any, Dict
import asyncio
import sys

from stockops.data.historical.providers import get_historical_service
from stockops.data.streaming.providers import get_streaming_service
from stockops.data.controller import Controller

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger(__name__)

def run_controller_task(command: Dict[str, Any], command_type: str, provider: str = "EODHD") -> None:
    logger.info("Preparing to run controller with type '%s' and command: %s", command_type, command)

    try:
        if command_type == "start_stream":
            service = get_streaming_service(provider)
            controller = Controller(command=command, streaming_service=service)
        elif command_type == "fetch_historical":
            service = get_historical_service(provider)
            controller = Controller(command=command, historical_service=service)
        else:
            raise ValueError(f"Unsupported command type: {command_type}")

        asyncio.run(controller())
        logger.info("Controller finished for command: %s", command)

    except Exception:
        logger.exception("Controller task failed for command: %s", command)
        raise

def controller_driver_flow(
    command: Dict[str, Any],
    command_type: str,
    provider: str = "EODHD"
) -> None:
    logger.info("Running controller_driver_flow...")

    run_controller_task(command, command_type, provider)

provider = 'EODHD'

# command = {'ticker': 'SPY.US', 'interval': '1m', 'start': '2025-07-02 09:30', 'end': '2025-07-02 16:00'}
# command_type = 'fetch_historical'
# command = {'ticker': 'SPY.US', 'interval': 'd', 'start': '2025-07-02 09:30', 'end': '2025-07-02 16:00'}
# command_type = 'fetch_historical'
# command_type="start_stream"
# command = {"stream_type": "trades", "tickers": ['SPY'], "duration": 10}
command_type="start_stream"
command = {"stream_type": "quotes", "tickers": ['SPY'], "duration": 10}

controller_driver_flow(command, command_type, provider)
