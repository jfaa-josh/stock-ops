import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from stockops.data.streaming.streaming_service import StreamManager


@pytest.mark.asyncio
async def test_stream_manager_runs_and_stores_messages():
    # Arrange
    fake_ws_url = "wss://example.com/fake"
    fake_symbols = ["FAKE"]
    table_name = "test_table"

    with patch("stockops.data.streaming.streaming_service.SQLiteWriter") as MockWriter:
        mock_writer = MockWriter.return_value
        mock_writer.insert = MagicMock()

        with patch("stockops.data.streaming.streaming_service.websockets.connect") as mock_connect:

            class FakeWebSocket:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    pass

                async def send(self, msg):
                    payload = json.loads(msg)
                    assert payload["action"] == "subscribe"
                    assert payload["symbols"] == ",".join(fake_symbols)

                def __aiter__(self):
                    async def async_generator():
                        await asyncio.sleep(0.01)  # Allow event loop to run stream task
                        yield json.dumps({"price": 123.45, "symbol": "FAKE"})

                    return async_generator()

            mock_connect.return_value = FakeWebSocket()

            # Act
            manager = StreamManager(db_path=":memory:")
            manager.start_stream(fake_ws_url, fake_symbols, table_name)

            await asyncio.sleep(0.2)  # Give background task time to run
            await manager.stop_all_streams()

            # Debug output
            print("mock_writer.insert.call_args_list =", mock_writer.insert.call_args_list)

            # Assert
            mock_writer.insert.assert_called_with({"price": 123.45, "symbol": "FAKE"})
