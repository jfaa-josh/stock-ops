import asyncio
import builtins
import logging
import threading
from asyncio import Queue, Semaphore

from stockops.data.historical.base_historical_service import AbstractHistoricalService
from stockops.data.sql_db import WriterRegistry
from stockops.data.streaming.base_streaming_service import AbstractStreamingService

logger = logging.getLogger("controller")

# --- Core State ---
task_queue: Queue[dict[str, str | int | list[str]]] = Queue()
shutdown_event = asyncio.Event()
stream_semaphore: Semaphore = Semaphore(5)  # default fallback
max_streams_config: int = 5  # used for logging and limits

stream_manager: AbstractStreamingService | None = None
hist_manager: AbstractHistoricalService | None = None
active_streams: set[asyncio.Task] = set()

_controller_running = False
_controller_lock = threading.Lock()


# --- Controller Initialization ---
def init_controller(
    streaming_service: AbstractStreamingService, historical_service: AbstractHistoricalService, max_streams: int = 5
):
    global stream_manager, hist_manager, stream_semaphore, max_streams_config
    stream_manager = streaming_service
    hist_manager = historical_service
    stream_semaphore = Semaphore(max_streams)
    max_streams_config = max_streams


# --- Stream Execution Wrapper ---
async def _wrap_stream_command(command: dict):
    assert stream_manager is not None
    tickers = command.get("tickers", [])

    logger.info(f"Waiting to acquire stream slot for {tickers} (semaphore throttle)")
    try:
        async with stream_semaphore:
            logger.info(f"Launching stream for {tickers}")
            stream_manager.start_stream(command)
    except Exception as e:
        logger.error(f"Error while starting stream {tickers}: {e}", exc_info=True)


# --- Dispatcher Loop ---
async def task_dispatcher():
    while not shutdown_event.is_set():
        try:
            command = await asyncio.wait_for(task_queue.get(), timeout=1.0)
        except builtins.TimeoutError:
            if task_queue.empty() and not active_streams:
                logger.info("No more commands or active streams. Triggering shutdown.")
                shutdown_event.set()
            continue

        if command["type"] == "start_stream":
            if stream_manager is None:
                logger.warning("stream_manager not initialized. Ignoring command.")
                continue

            task = asyncio.create_task(_wrap_stream_command(command))
            active_streams.add(task)

            tickers = command.get("tickers", [])
            logger.info(f"Stream registered for {tickers} — {len(active_streams)} active")

            def _on_stream_done(t: asyncio.Task, tickers=tickers):
                active_streams.discard(t)
                logger.info(f"Stream completed for {tickers} — {len(active_streams)} remaining")
                if task_queue.empty() and not active_streams:
                    logger.info("All streams finished. Triggering shutdown.")
                    shutdown_event.set()

            task.add_done_callback(_on_stream_done)

        elif command["type"] == "fetch_historical":
            if hist_manager is None:
                logger.warning("hist_manager not initialized. Ignoring command.")
                continue
            hist_manager.start_historical_task(command)

        elif command["type"] == "shutdown":
            logger.info("Shutdown command received.")
            shutdown_event.set()

        else:
            logger.warning(f"Unknown command type: {command['type']}")


# --- Orchestration ---
async def orchestrate() -> None:
    dispatcher = asyncio.create_task(task_dispatcher())
    await shutdown_event.wait()

    if stream_manager:
        await stream_manager.stop_all_streams()
    else:
        logger.warning("stream_manager not initialized. Skipping stream shutdown.")

    if hist_manager:
        await hist_manager.wait_for_all()
    else:
        logger.warning("hist_manager not initialized. Skipping historical wait.")

    await WriterRegistry.shutdown_all()
    await dispatcher


# --- External Interfaces ---
async def send_command(command: dict[str, str | int | list[str]]) -> None:
    start_controller_in_thread()

    if task_queue.full():
        raise RuntimeError(f"Task queue is full — rejecting command: {command}")

    qsize = task_queue.qsize()
    if qsize > max_streams_config:
        logger.warning(f"Task queue size is {qsize} — approaching concurrency threshold ({max_streams_config})")

    await task_queue.put(command)


def start_controller_in_thread():
    global _controller_running

    with _controller_lock:
        if _controller_running:
            return
        _controller_running = True

    def run_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(orchestrate())

        global _controller_running
        with _controller_lock:
            _controller_running = False

    threading.Thread(target=run_loop, name="ControllerThread", daemon=True).start()


def stop_controller_from_thread():
    shutdown_event.set()
