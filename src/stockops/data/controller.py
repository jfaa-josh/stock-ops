import asyncio
from asyncio import Queue

from stockops.data.historical.base_historical_service import AbstractHistoricalService
from stockops.data.streaming.base_streaming_service import AbstractStreamingService

task_queue: Queue[dict[str, str]] = Queue()
shutdown_event = asyncio.Event()

# These will be injected from command calls
stream_manager: AbstractStreamingService | None = None
hist_manager: AbstractHistoricalService | None = None


def init_controller(streaming_service: AbstractStreamingService, historical_service: AbstractHistoricalService):
    global stream_manager, hist_manager
    stream_manager = streaming_service
    hist_manager = historical_service


async def task_dispatcher():
    while not shutdown_event.is_set():
        try:
            command = await asyncio.wait_for(task_queue.get(), timeout=1.0)
        except TimeoutError:
            print("[controller] No command in queue yet...")
            continue

        if command["type"] == "start_stream":
            if stream_manager is None:
                print("Warning: stream_manager is not initialized yet. Ignoring command.")
                continue
            stream_manager.start_stream(command)

        elif command["type"] == "fetch_historical":
            if hist_manager is None:
                print("Warning: hist_manager is not initialized yet. Ignoring command.")
                continue
            hist_manager.start_historical_task(command)

        elif command["type"] == "shutdown":
            shutdown_event.set()

        else:
            print(f"Unknown command type: {command['type']}")


async def orchestrate() -> None:
    dispatcher = asyncio.create_task(task_dispatcher())
    await shutdown_event.wait()

    if stream_manager is not None:
        await stream_manager.stop_all_streams()
    else:
        print("Warning: stream_manager not initialized. Skipping stream shutdown.")

    if hist_manager is not None:
        await hist_manager.wait_for_all()
    else:
        print("Warning: hist_manager not initialized. Skipping historical wait.")

    await dispatcher


async def send_command(command: dict[str, str]) -> None:
    await task_queue.put(command)
