import asyncio
import inspect
import logging
import threading
from asyncio import Queue, Semaphore
from collections.abc import Awaitable
from typing import Any, cast

from stockops.data.historical.base_historical_service import AbstractHistoricalService
from stockops.data.sql_db import WriterRegistry
from stockops.data.streaming.base_streaming_service import AbstractStreamingService

logger = logging.getLogger(__name__)

# --- Core State ---
# Asyncio queue will live in controller loop; we schedule puts via run_coroutine_threadsafe
task_queue: Queue[dict] = Queue()
shutdown_event = asyncio.Event()
stream_semaphore: Semaphore = Semaphore(5)
max_streams_config: int = 5

stream_manager: AbstractStreamingService | None = None
hist_manager: AbstractHistoricalService | None = None
active_streams: set[asyncio.Task] = set()

_controller_running = False
_controller_lock = threading.Lock()
dispatcher_ready = threading.Event()
controller_loop: asyncio.AbstractEventLoop | None = None


def init_controller(
    streaming_service: AbstractStreamingService, historical_service: AbstractHistoricalService, max_streams: int = 5
):
    logger.debug("init_controller: services & semaphore setup")
    global stream_manager, hist_manager, stream_semaphore, max_streams_config
    stream_manager = streaming_service
    hist_manager = historical_service
    stream_semaphore = Semaphore(max_streams)
    max_streams_config = max_streams
    logger.debug("init_controller: max_streams=%d", max_streams)


async def _wrap_stream_command(command: dict):
    logger.debug("_wrap_stream_command start: %r", command)
    assert stream_manager is not None, "stream_manager not initialized"
    tickers = command.get("tickers", [])

    logger.debug("waiting for semaphore: %r", tickers)
    async with stream_semaphore:
        logger.debug("semaphore acquired: %r", tickers)
        result: Any = stream_manager.start_stream(command)
        logger.debug("start_stream returned: %r", result)
        if isinstance(result, asyncio.Task) or inspect.isawaitable(result):
            logger.debug("awaiting result")
            await cast(Awaitable[Any], result)
            logger.debug("result complete")
        else:
            raise RuntimeError(f"start_stream did not return awaitable; got {type(result)}")
    logger.debug("_wrap_stream_command done: %r", tickers)


async def task_dispatcher():
    logger.debug("task_dispatcher started")
    idle_start = None
    while not shutdown_event.is_set():
        logger.debug("awaiting command…")
        try:
            dispatcher_ready.set()
            cmd = await asyncio.wait_for(task_queue.get(), timeout=1.0)
            dispatcher_ready.clear()
            logger.debug("got cmd: %r", cmd)
        except TimeoutError:
            dispatcher_ready.clear()
            logger.debug("get timeout")
            if task_queue.empty() and not active_streams:
                now = asyncio.get_event_loop().time()
                if idle_start is None:
                    idle_start = now
                elif now - idle_start > 10:
                    logger.info("idle timeout, shutting down")
                    shutdown_event.set()
            else:
                idle_start = None
            continue

        typ = cmd.get("type")
        logger.debug("processing type=%s", typ)
        if typ == "start_stream":
            if stream_manager is None:
                logger.warning("no stream_manager")
                continue
            t = asyncio.create_task(_wrap_stream_command(cmd))
            logger.debug("task created — yielding control to let it start")
            await asyncio.sleep(0)
            active_streams.add(t)
            logger.debug("task created %r", t)

            def on_done(fut: asyncio.Task, tl=cmd.get("tickers")):  # noqa
                logger.debug("on_done for %r", tl)
                active_streams.discard(fut)
                if task_queue.empty() and not active_streams:
                    logger.info("all done, shutting down")
                    shutdown_event.set()

            t.add_done_callback(on_done)

        elif typ == "fetch_historical":
            logger.debug("fetch_historical")
            if hist_manager:
                hist_manager.start_historical_task(cmd)

        elif typ == "shutdown":
            logger.info("shutdown command received")
            shutdown_event.set()

        else:
            logger.warning("unknown command type %r", typ)

    logger.debug("dispatcher exiting")


async def orchestrate():
    logger.debug("orchestrate start")
    dispatcher = asyncio.create_task(task_dispatcher())
    logger.debug("waiting for shutdown_event")
    await shutdown_event.wait()
    logger.debug("shutdown_event set")

    if stream_manager:
        logger.debug("stop_all_streams")
        await stream_manager.stop_all_streams()
    if hist_manager:
        logger.debug("wait_for_all historical tasks")
        await hist_manager.wait_for_all()

    dispatcher_ready.clear()
    logger.debug("shutdown WriterRegistry")
    await WriterRegistry.shutdown_all()
    logger.debug("awaiting dispatcher finish")
    await dispatcher
    logger.debug("orchestrate complete")


async def send_command(command: dict) -> None:
    logger.debug("send_command called: %r", command)
    start_controller_in_thread()
    logger.debug("waiting for dispatcher_ready")
    await asyncio.to_thread(dispatcher_ready.wait, 2.0)
    logger.debug("dispatcher_ready set")

    if controller_loop is None:
        raise RuntimeError("Controller loop not initialized")

    logger.debug("putting command into controller_loop")
    fut = asyncio.run_coroutine_threadsafe(task_queue.put(command), controller_loop)
    fut.result(timeout=2.0)
    logger.debug("command enqueued")


def start_controller_in_thread():
    global _controller_running, controller_loop
    logger.debug("start_controller_in_thread")
    with _controller_lock:
        if _controller_running:
            logger.debug("controller already running")
            return
        _controller_running = True

    def run():
        global controller_loop
        logger.debug("ControllerThread starting loop")
        loop = asyncio.new_event_loop()
        controller_loop = loop
        asyncio.set_event_loop(loop)
        loop.run_until_complete(orchestrate())
        logger.debug("ControllerThread loop complete")
        with _controller_lock:
            _controller_running = False

    t = threading.Thread(target=run, daemon=True, name="ControllerThread")
    t.start()
    logger.debug("spawned thread %r", t)


def stop_controller_from_thread():
    logger.debug("stop_controller invoked")
    shutdown_event.set()
