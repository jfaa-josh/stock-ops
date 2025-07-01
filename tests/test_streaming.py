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

    # Patch the SQLiteWriter so it doesn't hit the file system
    with patch("stockops.data.streaming.streaming_service.SQLiteWriter") as MockWriter:
        mock_writer = MockWriter.return_value
        mock_writer.insert = MagicMock()

        # Patch websockets.connect to simulate server behavior
        async def fake_websocket_handler(*args, **kwargs):
            class FakeWebSocket:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, *args):
                    pass

                async def send(self, msg):
                    # Validate the subscribe message
                    payload = json.loads(msg)
                    assert payload["action"] == "subscribe"
                    assert payload["symbols"] == "FAKE"

                async def __aiter__(self):
                    yield json.dumps({"price": 123.45, "symbol": "FAKE"})

            return FakeWebSocket()

        with patch("stockops.data.streaming.streaming_service.websockets.connect", new=fake_websocket_handler):
            # Act
            manager = StreamManager(db_path=":memory:")  # path unused due to mocking
            manager.start_stream(fake_ws_url, fake_symbols, table_name)

            # Let it run for a short duration
            await asyncio.sleep(0.2)
            await manager.stop_all_streams()

            # Assert
            mock_writer.insert.assert_called_with({"price": 123.45, "symbol": "FAKE"})
