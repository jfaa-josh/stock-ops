import logging
from typing import Any, Dict

from prefect import flow, task, get_run_logger
from stockops.data.historical.providers import get_historical_service
from stockops.data.streaming.providers import get_streaming_service
from stockops.data.controller import Controller

logger = logging.getLogger(__name__)


@task
def run_controller_task(command: Dict[str, Any], command_type: str, provider: str = "EODHD") -> None:
    logger = get_run_logger()
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

@flow(name="controller_driver_flow")
def controller_driver_flow(
    command: Dict[str, Any],
    command_type: str,
    provider: str = "EODHD"
) -> None:
    logger = get_run_logger()
    logger.info("Running controller_driver_flow...")

    run_controller_task(command, command_type, provider)
