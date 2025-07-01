"""Created on Sun Jun 15 15:35:25 2025

@author: JoshFody
"""

import asyncio
import json
from pathlib import Path

import websockets

from stockops.data.sql_db import SQLiteWriter


class StreamManager:
    def __init__(self):
        self.tasks: list[asyncio.Task] = []
        self.db_filepaths: list[Path] = []

    async def _stream_data(self, ws_url: str, exp_data: dict, symbols: list[str], db_filepath: Path, table_name: str):
        writer = SQLiteWriter(db_filepath, table_name)
        expected_keys = set(exp_data)  # Keys to filter streaming data for expected structure

        try:
            while True:
                try:
                    print(f"Connecting to {ws_url}")
                    async with websockets.connect(ws_url) as websocket:
                        subscribe_msg = {"action": "subscribe", "symbols": ",".join(symbols)}
                        await websocket.send(json.dumps(subscribe_msg))
                        print(f"Subscribed to {symbols} on {ws_url}")

                        async for message in websocket:
                            try:
                                data = json.loads(message)

                                if "status_code" in data and "message" in data:
                                    print(f"[{table_name}] Handshake: {data}")
                                    continue

                                if expected_keys.issubset(data):  # Store only if data contains expected keys
                                    writer.insert(data)
                                    print(f"[{table_name}] {data}")
                                else:
                                    print(f"[{table_name}] Ignored non-trade message: {data}")

                            except json.JSONDecodeError:
                                safe_message = (
                                    message.decode("utf-8", errors="replace")
                                    if isinstance(message, bytes)
                                    else str(message)
                                )
                                print(f"[{table_name}] Warning: Non-JSON message: {safe_message}")

                            except Exception as e:
                                print(f"[{table_name}] Error: {e}")

                except Exception as e:
                    print(f"[{table_name}] Connection error: {e} — retrying in 5 seconds.")
                    await asyncio.sleep(5)

        except asyncio.CancelledError:
            print(f"[{table_name}] Stream cancelled.")
        finally:
            writer.close()

    def start_stream(self, ws_url: str, exp_data: dict, symbols: list[str], db_filepath: Path, table_name: str):
        task = asyncio.create_task(self._stream_data(ws_url, exp_data, symbols, db_filepath, table_name))
        self.tasks.append(task)
        self.db_filepaths.append(db_filepath)

    async def stop_all_streams(self):
        print("Stopping all streams...")
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        print("All streams stopped.")

    def clear_tasks(self):
        self.tasks = []
