import logging
import sys
from typing import Any, Dict, List

from prefect import flow, task, get_run_logger, run_deployment
from stockops.data.historical.providers import get_historical_service
from stockops.data.streaming.providers import get_streaming_service
from stockops.data.controller import Controller


@task
async def run_controller_task(command: Dict[str, Any], provider: str = "EODHD") -> None:
    logger = get_run_logger()
    logger.info("Preparing to run controller for command: %s", command)

    typ = command.get("type")
    if typ == "start_stream":
        service = get_streaming_service(provider)
        controller = Controller(command=command, streaming_service=service)
    elif typ == "fetch_historical":
        service = get_historical_service(provider)
        controller = Controller(command=command, historical_service=service)
    else:
        raise ValueError(f"Unsupported command type: {typ}")

    await controller()
    logger.info("Controller finished for command: %s", command)


@flow(name="controller_driver_flow")
def controller_driver_flow(
    commands: List[Dict[str, Any]],
    command_type: str,
    provider: str = "EODHD"
) -> None:
    for cmd in commands:
        full_command = {"type": command_type, **cmd}
        run_controller_task.submit(full_command, provider)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    run_deployment(
        name="controller-driver-flow/local-controller",
        parameters={
            "commands": [
                {"stream_type": "trades", "tickers": ["SPY"], "duration": 10},
                {"stream_type": "quotes", "tickers": ["SPY"], "duration": 10}
            ],
            "command_type": "start_stream",
            "provider": "EODHD"
        },
    )

    run_deployment(
        name="controller-driver-flow/local-controller",
        parameters={
            "commands": [
                {
                    "ticker": "SPY.US",
                    "interval": "1m",
                    "start": "2025-07-02 09:30",
                    "end": "2025-07-02 16:00"
                },
                {
                    "ticker": "SPY.US",
                    "interval": "d",
                    "start": "2025-07-02 09:30",
                    "end": "2025-07-03 16:00"
                }
            ],
            "command_type": "fetch_historical",
            "provider": "EODHD"
        },
    )
