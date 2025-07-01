"""Created on Sun Jun 15 15:35:25 2025

@author: JoshFody
"""

import asyncio
import json

import websockets

from stockops.data.sql_db import SQLiteWriter


class StreamManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.tasks: list[asyncio.Task] = []

    async def _stream_data(self, ws_url: str, symbols: list[str], table_name: str):
        writer = SQLiteWriter(self.db_path, table_name)

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
                                writer.insert(data)
                                print(f"[{table_name}] {data}")
                            except json.JSONDecodeError:
                                # 🔧 Safely handle bytes or string message
                                if isinstance(message, bytes):
                                    safe_message = message.decode("utf-8", errors="replace")
                                else:
                                    safe_message = message
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

    def start_stream(self, ws_url: str, symbols: list[str], table_name: str):
        task = asyncio.create_task(self._stream_data(ws_url, symbols, table_name))
        self.tasks.append(task)

    async def stop_all_streams(self):
        print("Stopping all streams...")
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        print("All streams stopped.")

    def clear_tasks(self):
        self.tasks = []
