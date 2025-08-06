import logging
from typing import Any, Dict

from prefect import flow, task, get_run_logger
from stockops.data.historical.providers import get_historical_service
from stockops.data.streaming.providers import get_streaming_service
from stockops.data.controller import Controller

logger = logging.getLogger(__name__)


@task
async def run_controller_task(command: Dict[str, Any], command_type: str, provider: str = "EODHD") -> None:
    logger = get_run_logger()
    logger.info("Preparing to run controller with type '%s' and command: %s", command_type, command)

    if command_type == "start_stream":
        service = get_streaming_service(provider)
        controller = Controller(command=command, streaming_service=service)
    elif command_type == "fetch_historical":
        service = get_historical_service(provider)
        controller = Controller(command=command, historical_service=service)
    else:
        raise ValueError(f"Unsupported command type: {command_type}")

    await controller()
    logger.info("Controller finished for command: %s", command)


@flow(name="controller_driver_flow")
def controller_driver_flow(
    command: Dict[str, Any],
    command_type: str,
    provider: str = "EODHD"
) -> None:
    run_controller_task.submit(command, command_type, provider)

# if __name__ == "__main__":
#     # Serve this flow (no work pools, no deployments):
#     controller_driver_flow.serve(
#         name="controller_driver_schedule",        # friendly display name
#     )
