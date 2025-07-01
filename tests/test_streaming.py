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
                        yield json.dumps({"price": 123.45, "symbol": "FAKE"})

                    return async_generator()

            mock_connect.return_value = FakeWebSocket()

            with patch("asyncio.sleep", return_value=None):
                # Act
                manager = StreamManager(db_path=":memory:")  # path unused due to mocking
                manager.start_stream(fake_ws_url, fake_symbols, table_name)

                await asyncio.sleep(0.2)
                await manager.stop_all_streams()

                # Assert
                mock_writer.insert.assert_called_with({"price": 123.45, "symbol": "FAKE"})
