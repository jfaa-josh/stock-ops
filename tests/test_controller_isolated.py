# import asyncio
# import pytest
# from unittest.mock import AsyncMock, MagicMock, patch
# import logging

# from stockops.data import controller

# # Setup logging for test output
# logger = logging.getLogger("test_controller_internal")
# logger.setLevel(logging.INFO)
# handler = logging.StreamHandler()
# formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
# handler.setFormatter(formatter)
# logger.addHandler(handler)


# @pytest.mark.asyncio
# async def test_init_controller_configures_globals():
#     logger.info("🔧 [controller.py] Testing init_controller sets globals correctly...")
#     stream = MagicMock()
#     hist = MagicMock()
#     controller.init_controller(stream, hist, max_streams=3)

#     assert controller.stream_manager is stream
#     assert controller.hist_manager is hist
#     assert isinstance(controller.stream_semaphore, asyncio.Semaphore)
#     assert controller.max_streams_config == 3
#     logger.info("✅ [controller.py] init_controller passed — globals set as expected.")


# @pytest.mark.asyncio
# async def test_wrap_stream_command_success():
#     logger.info("🔁 [controller.py] Testing _wrap_stream_command with valid stream_manager...")
#     stream_mock = MagicMock()
#     controller.stream_manager = stream_mock
#     controller.stream_semaphore = asyncio.Semaphore(1)

#     command = {"tickers": ["SPY"], "type": "start_stream"}
#     await controller._wrap_stream_command(command)

#     stream_mock.start_stream.assert_called_once_with(command)
#     logger.info("✅ [controller.py] _wrap_stream_command successfully invoked start_stream().")


# @pytest.mark.asyncio
# async def test_wrap_stream_command_logs_error(caplog):
#     logger.info("💥 [controller.py] Testing _wrap_stream_command handles stream errors...")

#     class FailingStream:
#         def start_stream(self, command):  # <--- Synchronous to match prod
#             raise RuntimeError("boom")

#     controller.stream_manager = FailingStream()
#     controller.stream_semaphore = asyncio.Semaphore(1)

#     with caplog.at_level("ERROR"):
#         await controller._wrap_stream_command({"tickers": ["FAIL"], "type": "start_stream"})

#     # ✅ Match the full log message from controller.py
#     assert "Error while starting stream ['FAIL']: boom" in caplog.text
#     logger.info("✅ [controller.py] error handling test passed (error log was suppressed).")

# @pytest.mark.asyncio
# async def test_dispatcher_handles_all_command_types():
#     logger.info("🚦 [controller.py] Testing task_dispatcher with multiple command types...")

#     controller.task_queue = asyncio.Queue()
#     controller.active_streams = set()
#     controller.shutdown_event = asyncio.Event()
#     controller.stream_semaphore = asyncio.Semaphore(1)

#     # ✅ Use MagicMock for sync-style call
#     stream_manager = MagicMock()
#     stream_manager.start_stream = MagicMock()

#     # Historical is also treated as sync
#     hist_manager = MagicMock()
#     hist_manager.start_historical_task = MagicMock()

#     controller.stream_manager = stream_manager
#     controller.hist_manager = hist_manager

#     await controller.task_queue.put({"type": "start_stream", "tickers": ["SPY"]})
#     await controller.task_queue.put({"type": "fetch_historical", "ticker": "AAPL"})
#     await controller.task_queue.put({"type": "invalid_type"})
#     await controller.task_queue.put({"type": "shutdown"})

#     task = asyncio.create_task(controller.task_dispatcher())

#     await asyncio.wait_for(controller.shutdown_event.wait(), timeout=3)
#     await asyncio.sleep(0.1)  # let remaining dispatcher work flush

#     task.cancel()
#     try:
#         await task
#     except asyncio.CancelledError:
#         pass

#     stream_manager.start_stream.assert_called_once()
#     hist_manager.start_historical_task.assert_called_once()
#     logger.info("✅ [controller.py] task_dispatcher handled all command types correctly.")


# @pytest.mark.asyncio
# async def test_orchestrate_handles_shutdown_paths():
#     logger.info("🛑 [controller.py] Testing orchestrate() handles shutdown and cleanup...")

#     controller.shutdown_event = asyncio.Event()
#     controller.task_queue = asyncio.Queue()

#     controller.stream_manager = AsyncMock()
#     controller.hist_manager = AsyncMock()
#     controller.stream_manager.stop_all_streams = AsyncMock()
#     controller.hist_manager.wait_for_all = AsyncMock()

#     await controller.task_queue.put({"type": "shutdown"})

#     task = asyncio.create_task(controller.orchestrate())
#     controller.shutdown_event.set()
#     await asyncio.wait_for(task, timeout=3)

#     controller.stream_manager.stop_all_streams.assert_awaited_once()
#     controller.hist_manager.wait_for_all.assert_awaited_once()
#     logger.info("✅ [controller.py] orchestrate shut down services cleanly.")


# def test_start_controller_thread_runs_once():
#     logger.info("🔁 [controller.py] Testing start_controller_in_thread runs only once...")

#     with patch("stockops.data.controller.orchestrate", new=AsyncMock()):
#         controller._controller_running = False
#         controller.start_controller_in_thread()
#         controller.start_controller_in_thread()  # Should be skipped

#         asyncio.run(asyncio.sleep(0.1))  # Allow thread to terminate
#         assert controller._controller_running is False
#         logger.info("✅ [controller.py] start_controller_in_thread is thread-safe and idempotent.")


# def test_stop_controller_sets_shutdown():
#     logger.info("🔻 [controller.py] Testing stop_controller_from_thread sets shutdown_event...")
#     controller.shutdown_event = asyncio.Event()
#     assert not controller.shutdown_event.is_set()
#     controller.stop_controller_from_thread()
#     assert controller.shutdown_event.is_set()
#     logger.info("✅ [controller.py] stop_controller_from_thread triggered shutdown as expected.")
