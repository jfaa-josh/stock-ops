import logging
from typing import Any, Dict
import sys, os

from stockops.data.historical.providers import get_historical_service
from stockops.data.streaming.providers import get_streaming_service
from stockops.data.controller import Controller


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

        controller()
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

def test_local():
    # Logging setup
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    os.environ["TEST_SERVICES"] = "1" # Set to 1 print API call output rather than write
    provider = 'EODHD'

    # command = {'ticker': 'SPY', 'exchange': 'US', 'interval': '1h', 'start': '2025-07-02 09:30', 'end': '2025-07-03 16:00'}
    # command_type = 'fetch_historical'
    command = {'ticker': 'VOO', 'exchange': 'US', 'interval': 'd', 'start': '2024-10-25', 'end': '2024-11-04'}
    command_type = 'fetch_historical'
    # command_type="start_stream"
    # command = {"stream_type": "trades", "tickers": 'SPY', 'exchange': 'US', "duration": 20}
    # command_type="start_stream"
    # command = {"stream_type": "quotes", "tickers": 'SPY', 'exchange': 'US', "duration": 20}

    controller_driver_flow(command, command_type, provider)

def test_ci():
    os.environ['TEST_CI'] = '1' # No actual API call made
    provider = 'EODHD'

    for command, command_type in [
        ({'ticker': 'SPY', 'exchange': 'US', 'interval': '1h', 'start': '2025-07-02 09:30', 'end': '2025-07-03 16:00'},
        'fetch_historical'),
        ({'ticker': 'VOO', 'exchange': 'US', 'interval': 'd', 'start': '2024-10-25', 'end': '2024-11-04'},
        'fetch_historical'),
        ({"stream_type": "trades", "tickers": 'SPY', 'exchange': 'US', "duration": 20},
         "start_stream"),
        ({"stream_type": "quotes", "tickers": 'SPY', 'exchange': 'US', "duration": 20},
         "start_stream")
    ]:
        controller_driver_flow(command, command_type, provider)

if __name__ == "__main__":
    test_local()
